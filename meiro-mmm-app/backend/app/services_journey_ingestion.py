"""
Safe ingestion pipeline for attribution journey JSON.

- Schema detection: auto-detect v1 (array) vs v2 (envelope with schema_version).
- v1: array of journeys (legacy).
- v2: envelope with defaults + journeys (recommended).
- Internal model: v2 canonical. v1 is converted to v2 before storage.
- Validation: structured items (journeyIndex, fieldPath, severity, message, suggestedFix).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from app.services_revenue_config import extract_revenue_entries, normalize_revenue_config
from app.utils.taxonomy import load_taxonomy, normalize_touchpoint

MEIRO_PARSER_VERSION = "2026-03-20-v2"

CANONICAL_CHANNELS = frozenset({"google_ads", "meta_ads", "linkedin_ads", "email", "direct", "whatsapp"})
LONG_JOURNEY_THRESHOLD = 200

CHANNEL_ALIASES: Dict[str, str] = {
    "google": "google_ads",
    "meta": "meta_ads",
    "facebook": "meta_ads",
    "linkedin": "linkedin_ads",
    "wa": "whatsapp",
    "organic": "direct",
    "direct": "direct",
}


def detect_schema(raw: Any) -> Tuple[str, List[Any], Dict[str, Any]]:
    """
    Auto-detect v1 vs v2. Returns (schema_version, journeys_list, defaults).
    """
    defaults: Dict[str, Any] = {"timezone": "UTC", "currency": "EUR"}
    if isinstance(raw, dict):
        if raw.get("schema_version") == "2.0" and "journeys" in raw:
            defs = raw.get("defaults") or {}
            if isinstance(defs, dict):
                defaults.update(defs)
            journeys = raw.get("journeys")
            return ("2.0", journeys if isinstance(journeys, list) else [], defaults)
        if "journeys" in raw:
            j = raw["journeys"]
            return ("1.0", j if isinstance(j, list) else [], defaults)
    if isinstance(raw, list):
        first = raw[0] if raw else None
        if isinstance(first, dict) and (
            "journey_id" in first
            or "customer" in first
            or ("touchpoints" in first and "conversions" in first)
        ):
            return ("2.0", raw, defaults)
        return ("1.0", raw, defaults)
    return ("1.0", [], defaults)


def _parse_timestamp(ts: Any) -> Optional[datetime]:
    """Parse ISO date or datetime to datetime. Date-only becomes midnight UTC."""
    if ts is None:
        return None
    s = str(ts).strip()
    if not s:
        return None
    try:
        if "T" in s or len(s) > 10:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s + "T00:00:00+00:00")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _extract_path(record: Any, path: str, fallback: Any = None) -> Any:
    raw = (path or "").strip()
    if not raw:
        return fallback
    current = record
    for part in raw.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return fallback
        if current is None:
            return fallback
    return current


def _iso_datetime(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _canonicalize_channel(raw: str) -> str:
    lower = raw.strip().lower().replace(" ", "_").replace("-", "_")
    if lower in CANONICAL_CHANNELS:
        return lower
    return CHANNEL_ALIASES.get(lower, raw)


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", []):
            return value
    return None


def _resolve_candidate(candidates: List[Tuple[str, Any, bool]]) -> Tuple[Any, Optional[str], bool]:
    for source_label, value, inferred in candidates:
        if value not in (None, "", []):
            return value, source_label, inferred
    return None, None, False


def _candidate_timestamp(*values: Any) -> Optional[str]:
    for value in values:
        if value in (None, "", []):
            continue
        parsed = _parse_timestamp(value)
        if parsed is not None:
            return _iso_datetime(parsed)
    return None


def _normalize_meiro_dedup_config(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cfg = raw if isinstance(raw, dict) else {}
    dedup_mode = str(cfg.get("dedup_mode") or "balanced").strip().lower()
    if dedup_mode not in {"strict", "balanced", "aggressive"}:
        dedup_mode = "balanced"
    primary_dedup_key = str(cfg.get("primary_dedup_key") or "auto").strip().lower()
    if primary_dedup_key not in {"auto", "conversion_id", "order_id", "event_id"}:
        primary_dedup_key = "auto"
    fallback_raw = cfg.get("fallback_dedup_keys")
    fallback_keys: List[str] = []
    if isinstance(fallback_raw, list):
        values = fallback_raw
    elif isinstance(fallback_raw, str):
        values = [part.strip() for part in fallback_raw.split(",")]
    else:
        values = []
    for item in values:
        key = str(item or "").strip().lower()
        if key in {"conversion_id", "order_id", "event_id"} and key != primary_dedup_key and key not in fallback_keys:
            fallback_keys.append(key)
    if not fallback_keys:
        fallback_keys = ["conversion_id", "order_id", "event_id"]
        if primary_dedup_key in fallback_keys:
            fallback_keys = [key for key in fallback_keys if key != primary_dedup_key]

    try:
        dedup_interval_minutes = int(cfg.get("dedup_interval_minutes") or 5)
    except Exception:
        dedup_interval_minutes = 5
    dedup_interval_minutes = max(0, min(1440, dedup_interval_minutes))
    return {
        "dedup_mode": dedup_mode,
        "primary_dedup_key": primary_dedup_key,
        "fallback_dedup_keys": fallback_keys,
        "dedup_interval_minutes": dedup_interval_minutes,
    }


def _conversion_identity(
    conversion: Dict[str, Any],
    dedup_cfg: Dict[str, Any],
) -> Optional[str]:
    candidates = []
    primary = str(dedup_cfg.get("primary_dedup_key") or "auto")
    if primary == "auto":
        candidates.extend(["conversion_id", "order_id", "event_id"])
    else:
        candidates.append(primary)
    for key in dedup_cfg.get("fallback_dedup_keys") or []:
        key_norm = str(key or "").strip().lower()
        if key_norm in {"conversion_id", "order_id", "event_id"} and key_norm not in candidates:
            candidates.append(key_norm)
    for key in candidates:
        if key == "conversion_id":
            value = conversion.get("id")
        else:
            value = conversion.get(key)
        if value not in (None, "", []):
            return f"{key}:{value}"
    return None


def _deduplicate_touchpoints(
    touchpoints: List[Dict[str, Any]],
    dedup_cfg: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], int]:
    dedup_mode = str(dedup_cfg.get("dedup_mode") or "balanced")
    dedup_window_seconds = int(dedup_cfg.get("dedup_interval_minutes") or 0) * 60
    if dedup_window_seconds <= 0 or len(touchpoints) < 2:
        return touchpoints, 0

    filtered: List[Dict[str, Any]] = []
    removed = 0
    last_ts: Optional[datetime] = None
    last_identity: Optional[Tuple[str, str, str, str]] = None
    last_channel: Optional[str] = None

    for touchpoint in touchpoints:
        ts = _parse_timestamp(touchpoint.get("ts") or touchpoint.get("timestamp"))
        if ts is None:
            filtered.append(touchpoint)
            last_ts = None
            last_identity = None
            last_channel = str(touchpoint.get("channel") or "")
            continue

        campaign = touchpoint.get("campaign")
        if isinstance(campaign, dict):
            campaign = campaign.get("name")
        identity = (
            str(touchpoint.get("source") or ((touchpoint.get("utm") or {}).get("source") or "")).strip().lower(),
            str(touchpoint.get("medium") or ((touchpoint.get("utm") or {}).get("medium") or "")).strip().lower(),
            str(campaign or ((touchpoint.get("utm") or {}).get("campaign") or "")).strip().lower(),
            str((((touchpoint.get("meta") or {}).get("parser") or {}).get("field_sources") or {}).get("channel") or "").strip().lower(),
        )
        should_skip = False
        if last_ts is not None and (ts - last_ts).total_seconds() < dedup_window_seconds:
            same_channel = last_channel == str(touchpoint.get("channel") or "")
            if dedup_mode == "strict":
                should_skip = same_channel and last_identity == identity
            elif dedup_mode == "aggressive":
                should_skip = same_channel or (
                    last_identity is not None
                    and identity[:3] == last_identity[:3]
                    and any(identity[:3])
                )
            else:
                should_skip = same_channel
        if should_skip:
            removed += 1
            continue

        filtered.append(touchpoint)
        last_ts = ts
        last_identity = identity
        last_channel = str(touchpoint.get("channel") or "")
    return filtered, removed


def _deduplicate_conversions(
    conversions: List[Dict[str, Any]],
    dedup_cfg: Dict[str, Any],
    *,
    seen_conversion_keys: Set[str],
) -> Tuple[List[Dict[str, Any]], int]:
    filtered: List[Dict[str, Any]] = []
    removed = 0
    for conversion in conversions:
        dedup_key = _conversion_identity(conversion, dedup_cfg)
        if dedup_key and dedup_key in seen_conversion_keys:
            removed += 1
            continue
        if dedup_key:
            seen_conversion_keys.add(dedup_key)
        filtered.append(conversion)
    return filtered, removed


def _touchpoint_list_from_profile(profile: Dict[str, Any], mapping: Dict[str, Any]) -> List[Any]:
    raw = _extract_path(profile, str(mapping.get("touchpoint_attr") or "touchpoints"))
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return parsed
    fallback: List[Dict[str, Any]] = []
    for key in ("first_touch_channel", "last_touch_channel", "channel", "source"):
        if profile.get(key) not in (None, "", []):
            fallback.append(
                {
                    "channel": profile.get(key),
                    "source": profile.get("source"),
                    "medium": profile.get("medium"),
                    "utm_source": profile.get("utm_source"),
                    "utm_medium": profile.get("utm_medium"),
                    "utm_campaign": profile.get("utm_campaign"),
                    "utm_content": profile.get("utm_content"),
                    "campaign": profile.get("campaign"),
                    "timestamp": _candidate_timestamp(
                        profile.get("timestamp"),
                        profile.get("event_date"),
                        profile.get("date"),
                    ),
                }
            )
    return fallback


def _looks_like_canonical_v2_journey(raw: Dict[str, Any]) -> bool:
    if "journey_id" in raw or "customer" in raw:
        return True
    touchpoints = raw.get("touchpoints")
    conversions = raw.get("conversions")
    if not isinstance(touchpoints, list) or not isinstance(conversions, list):
        return False
    if touchpoints:
        first_touchpoint = touchpoints[0]
        if not isinstance(first_touchpoint, dict):
            return False
        if "ts" not in first_touchpoint or "channel" not in first_touchpoint:
            return False
    if conversions:
        first_conversion = conversions[0]
        if not isinstance(first_conversion, dict):
            return False
        if "ts" not in first_conversion or "name" not in first_conversion:
            return False
    return True


def _build_v2_touchpoints(
    profile: Dict[str, Any],
    mapping: Dict[str, Any],
    *,
    idx: int,
) -> List[Dict[str, Any]]:
    taxonomy = load_taxonomy()
    out: List[Dict[str, Any]] = []
    for ti, raw_tp in enumerate(_touchpoint_list_from_profile(profile, mapping)):
        if isinstance(raw_tp, str):
            ts = _candidate_timestamp(profile.get("timestamp"), profile.get("event_date"), profile.get("date"))
            out.append(
                {
                    "id": f"tp_{idx}_{ti}",
                    "ts": ts or _iso_datetime(datetime.now(timezone.utc)),
                    "channel": _canonicalize_channel(str(raw_tp)),
                    "meta": {
                        "parser": {
                            "used_inferred_mapping": True,
                            "field_sources": {
                                "channel": "fallback:string_touchpoint",
                                "timestamp": "fallback:profile_timestamp" if ts else "fallback:generated_now",
                            },
                        }
                    },
                }
            )
            continue
        if not isinstance(raw_tp, dict):
            continue
        timestamp_value, timestamp_source, timestamp_inferred = _resolve_candidate(
            [
                (f"configured:{str(mapping.get('timestamp_field') or 'timestamp')}", _extract_path(raw_tp, str(mapping.get("timestamp_field") or "timestamp")), False),
                ("fallback:touchpoint.ts", raw_tp.get("ts"), True),
                ("fallback:touchpoint.timestamp", raw_tp.get("timestamp"), True),
                ("fallback:touchpoint.event_date", raw_tp.get("event_date"), True),
                ("fallback:touchpoint.date", raw_tp.get("date"), True),
                ("fallback:profile.timestamp", profile.get("timestamp"), True),
                ("fallback:profile.event_date", profile.get("event_date"), True),
                ("fallback:profile.date", profile.get("date"), True),
            ]
        )
        raw_timestamp = _candidate_timestamp(timestamp_value)
        channel_value, channel_source, channel_inferred = _resolve_candidate(
            [
                (f"configured:{str(mapping.get('channel_field') or 'channel')}", _extract_path(raw_tp, str(mapping.get("channel_field") or "channel")), False),
                ("fallback:touchpoint.channel", raw_tp.get("channel"), True),
                ("fallback:touchpoint.source", raw_tp.get("source"), True),
                ("fallback:touchpoint.utm_source", raw_tp.get("utm_source"), True),
                ("fallback:literal_unknown", "unknown", True),
            ]
        )
        source_value, source_source, source_inferred = _resolve_candidate(
            [
                (f"configured:{str(mapping.get('source_field') or 'source')}", _extract_path(raw_tp, str(mapping.get("source_field") or "source")), False),
                ("fallback:touchpoint.source", raw_tp.get("source"), True),
            ]
        )
        medium_value, medium_source, medium_inferred = _resolve_candidate(
            [
                (f"configured:{str(mapping.get('medium_field') or 'medium')}", _extract_path(raw_tp, str(mapping.get("medium_field") or "medium")), False),
                ("fallback:touchpoint.medium", raw_tp.get("medium"), True),
            ]
        )
        campaign_value, campaign_source, campaign_inferred = _resolve_candidate(
            [
                (f"configured:{str(mapping.get('campaign_field') or 'campaign')}", _extract_path(raw_tp, str(mapping.get("campaign_field") or "campaign")), False),
                ("fallback:touchpoint.campaign", raw_tp.get("campaign"), True),
                ("fallback:touchpoint.utm_campaign", raw_tp.get("utm_campaign"), True),
            ]
        )
        normalized = normalize_touchpoint(
            {
                "channel": channel_value,
                "source": source_value,
                "medium": medium_value,
                "utm_source": raw_tp.get("utm_source"),
                "utm_medium": raw_tp.get("utm_medium"),
                "utm_campaign": raw_tp.get("utm_campaign"),
                "utm_content": raw_tp.get("utm_content"),
                "campaign": campaign_value,
                "adset": raw_tp.get("adset"),
                "ad": raw_tp.get("ad"),
                "creative": _first_present(raw_tp.get("creative"), raw_tp.get("utm_content")),
                "timestamp": raw_timestamp,
            },
            taxonomy,
        )
        tp: Dict[str, Any] = {
            "id": str(raw_tp.get("id") or f"tp_{idx}_{ti}"),
            "ts": raw_timestamp or _iso_datetime(datetime.now(timezone.utc)),
            "channel": str(normalized.get("channel") or "unknown"),
        }
        if normalized.get("source") not in (None, "", []):
            tp["source"] = normalized.get("source")
        if normalized.get("medium") not in (None, "", []):
            tp["medium"] = normalized.get("medium")
        campaign = normalized.get("campaign")
        if campaign not in (None, "", []):
            tp["campaign"] = campaign if isinstance(campaign, dict) else {"name": str(campaign)}
        utm: Dict[str, Any] = {}
        if raw_tp.get("utm_source") not in (None, "", []):
            utm["source"] = raw_tp.get("utm_source")
        if raw_tp.get("utm_medium") not in (None, "", []):
            utm["medium"] = raw_tp.get("utm_medium")
        if raw_tp.get("utm_campaign") not in (None, "", []):
            utm["campaign"] = raw_tp.get("utm_campaign")
        if raw_tp.get("utm_content") not in (None, "", []):
            utm["content"] = raw_tp.get("utm_content")
        if utm:
            tp["utm"] = utm
        tp["meta"] = {
            "parser": {
                "used_inferred_mapping": bool(
                    timestamp_inferred
                    or channel_inferred
                    or source_inferred
                    or medium_inferred
                    or campaign_inferred
                ),
                "field_sources": {
                    "timestamp": timestamp_source or ("fallback:generated_now" if not raw_timestamp else None),
                    "channel": channel_source,
                    "source": source_source,
                    "medium": medium_source,
                    "campaign": campaign_source,
                },
            }
        }
        out.append(tp)
    return out


def _build_v2_conversions(
    profile: Dict[str, Any],
    mapping: Dict[str, Any],
    revenue_config: Dict[str, Any],
    *,
    idx: int,
    last_touch_ts: Optional[str],
) -> List[Dict[str, Any]]:
    raw_conversions = profile.get("conversions")
    source_items: List[Dict[str, Any]] = []
    if isinstance(raw_conversions, list) and raw_conversions:
        source_items = [item for item in raw_conversions if isinstance(item, dict)]
    else:
        converted = profile.get("converted")
        if isinstance(converted, str):
            converted = converted.strip().lower() in {"true", "1", "yes"}
        raw_value = _first_present(
            _extract_path(profile, str(mapping.get("value_attr") or "conversion_value")),
            profile.get("conversion_value"),
            profile.get("revenue"),
            profile.get("value"),
        )
        if converted is False and raw_value in (None, "", []):
            return []
        source_items = [profile]

    default_name = str(
        profile.get("kpi_type")
        or profile.get("conversion_key")
        or ((revenue_config.get("conversion_names") or ["conversion"])[0])
        or "conversion"
    ).strip() or "conversion"
    currency_field = str(
        mapping.get("currency_field")
        or revenue_config.get("currency_field_path")
        or "currency"
    )
    conversions: List[Dict[str, Any]] = []
    for ci, raw_conv in enumerate(source_items):
        value, value_source, value_inferred = _resolve_candidate(
            [
                (f"configured:{str(mapping.get('value_attr') or 'conversion_value')}", _extract_path(raw_conv, str(mapping.get("value_attr") or "conversion_value")), False),
                ("fallback:conversion.value", raw_conv.get("value"), True),
                ("fallback:conversion.conversion_value", raw_conv.get("conversion_value"), True),
            ]
        )
        currency, currency_source, currency_inferred = _resolve_candidate(
            [
                (f"configured:{currency_field}", _extract_path(raw_conv, currency_field), False),
                ("fallback:conversion.currency", raw_conv.get("currency"), True),
                (f"fallback:profile.{currency_field}", _extract_path(profile, currency_field), True),
                ("fallback:profile.currency", profile.get("currency"), True),
                ("fallback:revenue_base_currency", revenue_config.get("base_currency"), True),
                ("fallback:literal_eur", "EUR", True),
            ]
        )
        ts_value, ts_source, ts_inferred = _resolve_candidate(
            [
                ("fallback:conversion.ts", raw_conv.get("ts"), True),
                ("fallback:conversion.timestamp", raw_conv.get("timestamp"), True),
                ("fallback:conversion.event_date", raw_conv.get("event_date"), True),
                ("fallback:conversion.date", raw_conv.get("date"), True),
                ("fallback:profile.conversion_ts", profile.get("conversion_ts"), True),
                ("fallback:profile.timestamp", profile.get("timestamp"), True),
                ("fallback:last_touch_ts", last_touch_ts, True),
                ("fallback:generated_now", datetime.now(timezone.utc), True),
            ]
        )
        name_value, name_source, name_inferred = _resolve_candidate(
            [
                ("fallback:conversion.name", raw_conv.get("name"), True),
                ("fallback:conversion.event_name", raw_conv.get("event_name"), True),
                ("fallback:conversion.type", raw_conv.get("type"), True),
                ("fallback:profile.kpi_type", profile.get("kpi_type"), True),
                ("fallback:profile.conversion_key", profile.get("conversion_key"), True),
                ("fallback:default_name", default_name, True),
            ]
        )
        conversion = {
            "id": str(
                raw_conv.get("id")
                or raw_conv.get("conversion_id")
                or raw_conv.get("event_id")
                or raw_conv.get("order_id")
                or f"cv_{idx}_{ci}"
            ),
            "ts": _candidate_timestamp(ts_value),
            "name": str(name_value).strip() or default_name,
            "currency": str(currency or "EUR"),
        }
        if value not in (None, "", []):
            conversion["value"] = value
        if raw_conv.get("order_id") not in (None, "", []):
            conversion["order_id"] = raw_conv.get("order_id")
        if raw_conv.get("event_id") not in (None, "", []):
            conversion["event_id"] = raw_conv.get("event_id")
        conversion["meta"] = {
            "parser": {
                "used_inferred_mapping": bool(value_inferred or currency_inferred or ts_inferred or name_inferred),
                "field_sources": {
                    "value": value_source,
                    "currency": currency_source,
                    "ts": ts_source,
                    "name": name_source,
                },
            }
        }
        conversions.append(conversion)
    return conversions


def canonicalize_meiro_profiles(
    raw_input: Any,
    mapping: Optional[Dict[str, Any]] = None,
    revenue_config: Optional[Dict[str, Any]] = None,
    dedup_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Normalize mixed Meiro payloads into internal v2 journeys and attach revenue entries."""
    mapping_cfg = {
        "touchpoint_attr": "touchpoints",
        "value_attr": "conversion_value",
        "id_attr": "customer_id",
        "channel_field": "channel",
        "timestamp_field": "timestamp",
        "source_field": "source",
        "medium_field": "medium",
        "campaign_field": "campaign",
        "currency_field": "currency",
    }
    if isinstance(mapping, dict):
        mapping_cfg.update({k: v for k, v in mapping.items() if v not in (None, "")})
    revenue_cfg = normalize_revenue_config(revenue_config)
    dedup_cfg = _normalize_meiro_dedup_config(dedup_config)
    _, journeys_list, defaults = detect_schema(raw_input)
    seen_conversion_keys: Set[str] = set()

    prepared: List[Dict[str, Any]] = []
    for idx, raw in enumerate(journeys_list):
        if not isinstance(raw, dict):
            prepared.append(raw)
            continue
        if _looks_like_canonical_v2_journey(raw):
            raw_touchpoints = raw.get("touchpoints")
            if isinstance(raw_touchpoints, list):
                deduped_touchpoints, removed_touchpoints = _deduplicate_touchpoints(raw_touchpoints, dedup_cfg)
                if removed_touchpoints:
                    raw = dict(raw)
                    raw["touchpoints"] = deduped_touchpoints
                    raw_meta = dict(raw.get("meta") or {})
                    parser_meta = dict(raw_meta.get("parser") or {})
                    parser_meta["dedup_removed_touchpoints"] = removed_touchpoints
                    parser_meta["dedup_mode"] = dedup_cfg.get("dedup_mode")
                    raw_meta["parser"] = parser_meta
                    raw["meta"] = raw_meta
            raw_conversions = raw.get("conversions")
            if isinstance(raw_conversions, list):
                deduped_conversions, removed_conversions = _deduplicate_conversions(raw_conversions, dedup_cfg, seen_conversion_keys=seen_conversion_keys)
                if removed_conversions:
                    raw = dict(raw)
                    raw["conversions"] = deduped_conversions
                    raw_meta = dict(raw.get("meta") or {})
                    parser_meta = dict(raw_meta.get("parser") or {})
                    parser_meta["dedup_removed_conversions"] = removed_conversions
                    parser_meta["dedup_primary_key"] = dedup_cfg.get("primary_dedup_key")
                    raw_meta["parser"] = parser_meta
                    raw["meta"] = raw_meta
            prepared.append(raw)
            continue
        touchpoints = _build_v2_touchpoints(raw, mapping_cfg, idx=idx)
        touchpoints, removed_touchpoints = _deduplicate_touchpoints(touchpoints, dedup_cfg)
        last_touch_ts = touchpoints[-1]["ts"] if touchpoints else None
        conversions = _build_v2_conversions(raw, mapping_cfg, revenue_cfg, idx=idx, last_touch_ts=last_touch_ts)
        conversions, removed_conversions = _deduplicate_conversions(conversions, dedup_cfg, seen_conversion_keys=seen_conversion_keys)
        customer_id = _first_present(
            _extract_path(raw, str(mapping_cfg.get("id_attr") or "customer_id")),
            raw.get("customer_id"),
            raw.get("profile_id"),
            raw.get("id"),
            f"anon-{idx}",
        )
        customer_id_source = (
            f"configured:{str(mapping_cfg.get('id_attr') or 'customer_id')}"
            if _extract_path(raw, str(mapping_cfg.get("id_attr") or "customer_id")) not in (None, "", [])
            else (
                "fallback:customer_id"
                if raw.get("customer_id") not in (None, "", [])
                else "fallback:profile_id"
                if raw.get("profile_id") not in (None, "", [])
                else "fallback:id"
                if raw.get("id") not in (None, "", [])
                else "fallback:generated_anon"
            )
        )
        prepared.append(
            {
                "journey_id": str(raw.get("journey_id") or f"j_{uuid.uuid4().hex[:12]}"),
                "customer": {"id": str(customer_id), "type": "profile_id"},
                "defaults": defaults,
                "touchpoints": touchpoints,
                "conversions": conversions,
                "kpi_type": str(raw.get("kpi_type") or "") or None,
                "meta": {
                    "parser": {
                        "used_inferred_mapping": customer_id_source != f"configured:{str(mapping_cfg.get('id_attr') or 'customer_id')}",
                        "field_sources": {"customer_id": customer_id_source},
                        "dedup_removed_touchpoints": removed_touchpoints,
                        "dedup_removed_conversions": removed_conversions,
                        "dedup_mode": dedup_cfg.get("dedup_mode"),
                        "dedup_primary_key": dedup_cfg.get("primary_dedup_key"),
                    }
                },
                "_schema": "2.0",
            }
        )

    result = validate_and_normalize(prepared)
    for journey in result.get("valid_journeys", []):
        entries = extract_revenue_entries(
            journey,
            revenue_cfg,
            fallback_conversion_id=str(journey.get("journey_id") or ""),
        )
        if entries:
            journey["_revenue_entries"] = entries
        if not journey.get("kpi_type"):
            conversions = journey.get("conversions") or []
            if conversions and isinstance(conversions[0], dict) and conversions[0].get("name"):
                journey["kpi_type"] = str(conversions[0]["name"])
        parser_meta = dict((journey.get("meta") or {}).get("parser") or {})
        tp_parser = [((tp.get("meta") or {}).get("parser") or {}) for tp in (journey.get("touchpoints") or []) if isinstance(tp, dict)]
        conv_parser = [((conv.get("meta") or {}).get("parser") or {}) for conv in (journey.get("conversions") or []) if isinstance(conv, dict)]
        parser_items = [parser_meta, *tp_parser, *conv_parser]
        inferred_items = sum(1 for item in parser_items if item.get("used_inferred_mapping"))
        parser_meta["inferred_items"] = inferred_items
        parser_meta["total_items"] = len(parser_items)
        parser_meta["confidence"] = round((1.0 - (inferred_items / float(len(parser_items) or 1))), 4)
        parser_meta["used_inferred_mapping"] = inferred_items > 0
        journey.setdefault("meta", {})["parser"] = parser_meta
    return result


def _add_validation(
    items: List[Dict[str, Any]],
    journey_index: int,
    field_path: str,
    severity: str,
    message: str,
    suggested_fix: str,
) -> None:
    items.append({
        "journeyIndex": journey_index,
        "fieldPath": field_path,
        "severity": severity,
        "message": message,
        "suggestedFix": suggested_fix,
    })


def _v1_to_internal_v2(
    norm: Dict[str, Any],
    idx: int,
    defaults: Dict[str, Any],
) -> Dict[str, Any]:
    """Convert validated v1 (legacy) format to internal v2 model."""
    cid = norm.get("customer_id", f"anon-{idx}")
    tps = norm.get("touchpoints", [])
    converted = norm.get("converted", True)
    cv = norm.get("conversion_value", 0.0)
    currency = defaults.get("currency", "EUR")

    v2_touchpoints: List[Dict[str, Any]] = []
    last_ts = None
    for ti, tp in enumerate(tps):
        ts_str = tp.get("timestamp", "")
        ts_dt = _parse_timestamp(ts_str)
        if ts_dt:
            last_ts = ts_dt
        nt: Dict[str, Any] = {
            "id": tp.get("id", f"tp_{idx}_{ti}"),
            "ts": ts_str or _iso_datetime(datetime.now(timezone.utc)),
            "channel": tp.get("channel", "unknown"),
        }
        camp = tp.get("campaign")
        if camp:
            nt["campaign"] = {"name": camp} if isinstance(camp, str) else camp
        v2_touchpoints.append(nt)

    v2_conversions: List[Dict[str, Any]] = []
    if converted and (cv or cv == 0):
        conv_ts = last_ts or datetime.now(timezone.utc)
        v2_conversions.append({
            "id": f"cv_{idx}_0",
            "ts": _iso_datetime(conv_ts),
            "name": norm.get("kpi_type", "conversion"),
            "value": float(cv),
            "currency": currency,
        })

    return {
        "journey_id": norm.get("journey_id", f"j_{uuid.uuid4().hex[:12]}"),
        "customer": {"id": cid, "type": "customer_id"},
        "defaults": defaults,
        "touchpoints": v2_touchpoints,
        "conversions": v2_conversions,
        "_schema": "2.0",
    }


def _validate_and_normalize_v1(
    raw: Any,
    idx: int,
    validation_items: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Validate v1 journey, return legacy format (to be converted to v2) or None."""
    if not isinstance(raw, dict):
        _add_validation(
            validation_items, idx, "", "error",
            f"Journey must be an object, got {type(raw).__name__}",
            "Each item must have customer_id, touchpoints, converted.",
        )
        return None

    norm: Dict[str, Any] = {}
    has_error = False

    cid = raw.get("customer_id")
    if cid is None:
        _add_validation(validation_items, idx, "customer_id", "error", "Required field 'customer_id' is missing.", "Add \"customer_id\": \"c001\".")
        has_error = True
        norm["customer_id"] = f"anon-{idx}"
    elif not isinstance(cid, str):
        _add_validation(validation_items, idx, "customer_id", "error", f"customer_id must be a string.", "Use a string value.")
        has_error = True
        norm["customer_id"] = str(cid)
    else:
        norm["customer_id"] = cid.strip() or f"anon-{idx}"

    tps_raw = raw.get("touchpoints")
    if tps_raw is None:
        _add_validation(validation_items, idx, "touchpoints", "error", "Required field 'touchpoints' is missing.", "Add touchpoints array.")
        has_error = True
        norm["touchpoints"] = []
    elif not isinstance(tps_raw, list):
        _add_validation(validation_items, idx, "touchpoints", "error", "touchpoints must be an array.", "Use an array.")
        has_error = True
        norm["touchpoints"] = []
    else:
        touchpoints: List[Dict[str, Any]] = []
        timestamps: List[Tuple[int, datetime]] = []
        for ti, tp in enumerate(tps_raw):
            if not isinstance(tp, dict):
                _add_validation(validation_items, idx, f"touchpoints[{ti}]", "error", "Touchpoint must be an object.", "Each touchpoint needs channel and timestamp.")
                has_error = True
                continue
            ch = tp.get("channel")
            if ch is None or (isinstance(ch, str) and not ch.strip()):
                _add_validation(validation_items, idx, f"touchpoints[{ti}].channel", "error", "Required field 'channel' is missing.", "Add channel string.")
                has_error = True
                ch = "unknown"
            ch_str = str(ch).strip() if ch else "unknown"
            canonical = _canonicalize_channel(ch_str)
            ts_raw = tp.get("timestamp")
            ts_dt = _parse_timestamp(ts_raw)
            if ts_dt is None:
                _add_validation(validation_items, idx, f"touchpoints[{ti}].timestamp", "error", "Invalid timestamp.", "Use ISO date or datetime.")
                has_error = True
                ts_dt = datetime.now(timezone.utc)
            nt = {"channel": canonical, "timestamp": _iso_datetime(ts_dt), "campaign": tp.get("campaign") if isinstance(tp.get("campaign"), str) else None}
            touchpoints.append(nt)
            timestamps.append((ti, ts_dt))
        if len(timestamps) > 1:
            sorted_idx = sorted(range(len(touchpoints)), key=lambda i: timestamps[i][1])
            touchpoints = [touchpoints[i] for i in sorted_idx]
        norm["touchpoints"] = touchpoints

    conv = raw.get("converted")
    if conv is None:
        _add_validation(validation_items, idx, "converted", "error", "Required field 'converted' is missing.", "Add converted: true or false.")
        has_error = True
        norm["converted"] = True
    elif not isinstance(conv, bool):
        norm["converted"] = bool(conv)
        has_error = True
    else:
        norm["converted"] = conv

    if norm.get("converted", True):
        cv = raw.get("conversion_value")
        if cv is None:
            _add_validation(validation_items, idx, "conversion_value", "error", "conversion_value required when converted=true.", "Add a number >= 0.")
            has_error = True
            norm["conversion_value"] = 0.0
        else:
            try:
                v = float(cv)
                norm["conversion_value"] = max(0.0, v) if v >= 0 else 0.0
                if v < 0:
                    has_error = True
            except (TypeError, ValueError):
                _add_validation(validation_items, idx, "conversion_value", "error", "conversion_value must be a number.", "Use a numeric value.")
                has_error = True
                norm["conversion_value"] = 0.0
    else:
        norm["conversion_value"] = float(raw.get("conversion_value", 0) or 0)

    if raw.get("kpi_type") and isinstance(raw["kpi_type"], str):
        norm["kpi_type"] = raw["kpi_type"]

    if has_error:
        return None
    return norm


def _validate_and_normalize_v2(
    raw: Any,
    idx: int,
    defaults: Dict[str, Any],
    validation_items: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Validate and normalize v2 journey to internal v2 model."""
    if not isinstance(raw, dict):
        _add_validation(validation_items, idx, "", "error", "Journey must be an object.", "Use journey_id, customer, touchpoints, conversions.")
        return None

    has_error = False
    jid = raw.get("journey_id") or f"j_{uuid.uuid4().hex[:12]}"
    customer = raw.get("customer")
    if not customer or not isinstance(customer, dict):
        cid = raw.get("customer_id") or raw.get("profile_id") or f"anon-{idx}"
        customer = {"id": str(cid), "type": "profile_id"}
    else:
        cid = customer.get("id")
        if not cid:
            _add_validation(validation_items, idx, "customer.id", "error", "customer.id is required.", "Add customer.id string.")
            has_error = True
            customer = {"id": f"anon-{idx}", "type": customer.get("type", "profile_id")}
        else:
            customer = {"id": str(cid), "type": customer.get("type", "profile_id")}

    tps_raw = raw.get("touchpoints", [])
    if not isinstance(tps_raw, list):
        _add_validation(validation_items, idx, "touchpoints", "error", "touchpoints must be an array.", "Use array of touchpoint objects.")
        has_error = True
        tps_raw = []

    v2_touchpoints: List[Dict[str, Any]] = []
    timestamps: List[Tuple[int, datetime]] = []
    for ti, tp in enumerate(tps_raw):
        if not isinstance(tp, dict):
            has_error = True
            continue
        ts_raw = tp.get("ts") or tp.get("timestamp")
        ts_dt = _parse_timestamp(ts_raw)
        if ts_dt is None:
            _add_validation(validation_items, idx, f"touchpoints[{ti}].ts", "error", "Invalid timestamp.", "Use ISO datetime.")
            has_error = True
            ts_dt = datetime.now(timezone.utc)
        ch = tp.get("channel")
        if not ch or (isinstance(ch, str) and not ch.strip()):
            _add_validation(validation_items, idx, f"touchpoints[{ti}].channel", "error", "channel is required.", "Add channel string.")
            has_error = True
            ch = "unknown"
        ch_str = str(ch).strip()
        canonical = _canonicalize_channel(ch_str)
        camp = tp.get("campaign")
        nt: Dict[str, Any] = {
            "id": tp.get("id", f"tp_{idx}_{ti}"),
            "ts": _iso_datetime(ts_dt),
            "channel": canonical,
        }
        if camp:
            nt["campaign"] = camp if isinstance(camp, dict) else {"name": str(camp)}
        if tp.get("source"):
            nt["source"] = tp["source"]
        if tp.get("utm"):
            nt["utm"] = tp["utm"]
        if tp.get("cost"):
            nt["cost"] = tp["cost"]
        if tp.get("meta"):
            nt["meta"] = tp["meta"]
        v2_touchpoints.append(nt)
        timestamps.append((ti, ts_dt))

    if len(timestamps) > 1:
        v2_touchpoints = [v2_touchpoints[i] for i in sorted(range(len(v2_touchpoints)), key=lambda i: timestamps[i][1])]

    conv_raw = raw.get("conversions", [])
    if not isinstance(conv_raw, list):
        conv_raw = []
    v2_conversions: List[Dict[str, Any]] = []
    for ci, c in enumerate(conv_raw):
        if not isinstance(c, dict):
            continue
        cts = _parse_timestamp(c.get("ts"))
        if cts is None:
            cts = datetime.now(timezone.utc)
        value_present = c.get("value") not in (None, "", [])
        conversion_item = {
            "id": c.get("id", f"cv_{idx}_{ci}"),
            "ts": _iso_datetime(cts),
            "name": str(c.get("name", "conversion")),
            "currency": c.get("currency") or defaults.get("currency", "EUR"),
        }
        if value_present:
            try:
                val = float(c.get("value", 0) or 0)
            except (TypeError, ValueError):
                val = 0.0
            conversion_item["value"] = max(0.0, val)
        if c.get("meta"):
            conversion_item["meta"] = c["meta"]
        v2_conversions.append(conversion_item)

    if has_error:
        return None

    out = {
        "journey_id": jid,
        "customer": customer,
        "defaults": defaults,
        "touchpoints": v2_touchpoints,
        "conversions": v2_conversions,
        "_schema": "2.0",
    }
    if raw.get("meta"):
        out["meta"] = raw.get("meta")
    return out


def _apply_dq_rules(journeys: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Opinionated DQ checks: touchpoint after conversion, long journeys, unknown channels, currency."""
    items: List[Dict[str, Any]] = []
    for idx, j in enumerate(journeys):
        convs = j.get("conversions") or []
        tps = j.get("touchpoints") or []
        conv_times = [_parse_timestamp(c.get("ts")) for c in convs if c.get("ts")]
        conv_times = [t for t in conv_times if t is not None]
        if not conv_times:
            continue
        last_conv = max(conv_times)
        for ti, tp in enumerate(tps):
            ts = _parse_timestamp(tp.get("ts"))
            if ts and ts > last_conv:
                _add_validation(
                    items, idx, f"touchpoints[{ti}].ts", "error",
                    "Touchpoint timestamp is after conversion timestamp.",
                    "Remove or reorder touchpoints so none occur after conversion.",
                )
        if len(tps) > LONG_JOURNEY_THRESHOLD:
            _add_validation(
                items, idx, "touchpoints", "warning",
                f"Journey has {len(tps)} touchpoints (>200). May indicate data quality or config issue.",
                "Consider reducing lookback window or session gap in Meiro pull config.",
            )
        for ti, tp in enumerate(tps):
            ch = tp.get("channel", "unknown")
            if ch and ch not in CANONICAL_CHANNELS:
                _add_validation(
                    items, idx, f"touchpoints[{ti}].channel", "warning",
                    f"Unknown channel '{ch}'. Not in canonical set.",
                    "Add channel to taxonomy or mapping preset for consistent attribution.",
                )
        currencies = set()
        for c in convs:
            cur = c.get("currency")
            if cur:
                currencies.add(str(cur))
        if len(currencies) > 1:
            _add_validation(
                items, idx, "conversions", "warning",
                f"Currency inconsistency: {currencies}. Mixed currencies may skew aggregation.",
                "Normalize to single currency in defaults or per-conversion.",
            )
    return items


def validate_and_normalize(raw_input: Any) -> Dict[str, Any]:
    """
    Ingest raw JSON. Auto-detect v1 vs v2, validate, convert to internal v2.

    raw_input: Parsed JSON - either array (v1) or dict with schema_version+journeys (v2).

    Returns:
        valid_journeys: List of internal v2 journey dicts
        validation_items: Structured validation items
        import_summary: {total, valid, invalid, converted, channels_detected}
        items_detail: For drawer (original, normalized)
        schema_version: "1.0" or "2.0"
    """
    validation_items: List[Dict[str, Any]] = []
    valid_journeys: List[Dict[str, Any]] = []
    items_detail: List[Dict[str, Any]] = []
    channels_detected: set = set()

    schema_version, journeys_list, defaults = detect_schema(raw_input)

    if not isinstance(journeys_list, list):
        validation_items.append({
            "journeyIndex": -1, "fieldPath": "", "severity": "error",
            "message": "Input must be a JSON array of journeys or v2 envelope with 'journeys' array.",
            "suggestedFix": "Use v1: [...] or v2: {\"schema_version\": \"2.0\", \"journeys\": [...]}.",
        })
        return {
            "valid_journeys": [],
            "validation_items": validation_items,
            "import_summary": {"total": 0, "valid": 0, "invalid": 0, "converted": 0, "channels_detected": []},
            "items_detail": [],
            "schema_version": schema_version,
        }

    valid_index = 0
    for idx, raw in enumerate(journeys_list):
        original_copy = json.loads(json.dumps(raw)) if raw is not None else None
        norm: Optional[Dict[str, Any]] = None
        if schema_version == "2.0":
            norm = _validate_and_normalize_v2(raw, idx, defaults, validation_items)
        else:
            v1_norm = _validate_and_normalize_v1(raw, idx, validation_items)
            if v1_norm:
                norm = _v1_to_internal_v2(v1_norm, idx, defaults)

        valid = norm is not None
        detail: Dict[str, Any] = {"journeyIndex": idx, "original": original_copy, "normalized": norm, "valid": valid}
        if norm:
            detail["validIndex"] = valid_index
            valid_index += 1
            valid_journeys.append(norm)
            for tp in norm.get("touchpoints", []):
                channels_detected.add(tp.get("channel", "unknown"))
        items_detail.append(detail)

    converted = sum(1 for j in valid_journeys if j.get("conversions"))

    # Apply opinionated DQ rules
    dq_items = _apply_dq_rules(valid_journeys)
    validation_items.extend(dq_items)

    return {
        "valid_journeys": valid_journeys,
        "validation_items": validation_items,
        "import_summary": {
            "total": len(journeys_list),
            "valid": len(valid_journeys),
            "invalid": len(journeys_list) - len(valid_journeys),
            "converted": converted,
            "channels_detected": sorted(channels_detected),
        },
        "items_detail": items_detail,
        "schema_version": schema_version,
    }
