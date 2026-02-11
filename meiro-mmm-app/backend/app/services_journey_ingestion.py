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
from typing import Any, Dict, List, Optional, Tuple

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


def _iso_datetime(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _canonicalize_channel(raw: str) -> str:
    lower = raw.strip().lower().replace(" ", "_").replace("-", "_")
    if lower in CANONICAL_CHANNELS:
        return lower
    return CHANNEL_ALIASES.get(lower, raw)


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
        try:
            val = float(c.get("value", 0) or 0)
        except (TypeError, ValueError):
            val = 0.0
        v2_conversions.append({
            "id": c.get("id", f"cv_{idx}_{ci}"),
            "ts": _iso_datetime(cts),
            "name": str(c.get("name", "conversion")),
            "value": max(0.0, val),
            "currency": c.get("currency") or defaults.get("currency", "EUR"),
        })

    if has_error:
        return None

    return {
        "journey_id": jid,
        "customer": customer,
        "defaults": defaults,
        "touchpoints": v2_touchpoints,
        "conversions": v2_conversions,
        "_schema": "2.0",
    }


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
