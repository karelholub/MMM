"""Helpers to apply versioned ModelConfig to journeys.

This module keeps attribution math unchanged, but:
  - Applies time windows from config.windows to prune touchpoints
  - Annotates journeys with a conversion key (kpi_type) derived from config.conversions

The goal is to make attribution *config-aware* while remaining backward compatible
when no ModelConfig is supplied.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import hashlib
import uuid

import pandas as pd
from sqlalchemy.orm import Session

from .models_config_dq import ConversionPath
from .services_metrics import journey_outcome_summary
from .services_revenue_config import compute_payload_revenue_value, extract_revenue_entries, get_revenue_config


def _parse_ts(ts: Any):
    """Best-effort timestamp parsing. Returns pandas.Timestamp or None."""
    if not ts:
        return None
    try:
        return pd.to_datetime(ts, errors="coerce")
    except Exception:
        return None


def _tp_timestamp(tp: Dict[str, Any]) -> Any:
    """Get timestamp from touchpoint (v1: timestamp, v2: ts)."""
    return tp.get("ts") or tp.get("timestamp")


def _tp_interaction_type(tp: Dict[str, Any]) -> str:
    raw = str(tp.get("interaction_type") or "").strip().lower()
    if raw in {"impression", "click", "visit", "direct", "unknown"}:
        return raw
    if str(tp.get("channel") or "").strip().lower() == "direct":
        return "direct"
    return "unknown"


def _dedupe_click_preferred_touchpoints(touchpoints: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    click_keys = set()
    for tp in touchpoints:
        if _tp_interaction_type(tp) != "click":
            continue
        campaign = tp.get("campaign")
        if isinstance(campaign, dict):
            campaign = campaign.get("name")
        click_keys.add((str(tp.get("channel") or ""), str(campaign or "")))
    out: List[Dict[str, Any]] = []
    for tp in touchpoints:
        if _tp_interaction_type(tp) != "impression":
            out.append(tp)
            continue
        campaign = tp.get("campaign")
        if isinstance(campaign, dict):
            campaign = campaign.get("name")
        key = (str(tp.get("channel") or ""), str(campaign or ""))
        if key in click_keys:
            continue
        out.append(tp)
    return out


def classify_journey_interaction(journey: Dict[str, Any]) -> str:
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


def _selected_conversion_count(journey: Dict[str, Any], value_mode: str) -> float:
    summary = journey_outcome_summary(journey)
    return float(summary["net_conversions"] if value_mode == "net_only" else summary["gross_conversions"])


def journey_quality_score(journey: Dict[str, Any]) -> int:
    meta = journey.get("meta") if isinstance(journey.get("meta"), dict) else None
    quality = meta.get("quality") if isinstance(meta, dict) and isinstance(meta.get("quality"), dict) else None
    score = (
        journey.get("quality_score")
        if journey.get("quality_score") is not None
        else (quality.get("score") if quality else None)
    )
    try:
        return max(0, min(100, int(float(score))))
    except Exception:
        return 0


def filter_journeys_by_quality(
    journeys: List[Dict[str, Any]],
    min_quality_score: int = 0,
) -> List[Dict[str, Any]]:
    threshold = max(0, min(100, int(min_quality_score or 0)))
    if threshold <= 0:
        return journeys
    return [j for j in journeys if journey_quality_score(j) >= threshold]


def filter_journeys_by_windows(
    journeys: List[Dict[str, Any]],
    config_json: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Apply time windows from config.windows to touchpoints.

    Supports both v1 (timestamp) and v2 (ts) touchpoint format.
    """
    windows = config_json.get("windows") or {}
    click_days = float(windows.get("click_lookback_days") or 0)
    impression_days = float(windows.get("impression_lookback_days") or 0)
    attribution_cfg = config_json.get("attribution") or {}
    interaction_mode = str(attribution_cfg.get("interaction_mode") or "click_preferred").strip().lower()
    include_view_only = bool(attribution_cfg.get("include_impression_only_paths", False))
    if click_days <= 0 and impression_days <= 0:
        return journeys

    out: List[Dict[str, Any]] = []

    for j in journeys:
        tps = j.get("touchpoints") or []
        if not tps:
            continue
        parsed = [_parse_ts(_tp_timestamp(tp)) for tp in tps]
        valid_ts = [p for p in parsed if p is not None]
        if not valid_ts:
            # No usable timestamps; keep journey as-is to avoid dropping data silently
            out.append(j)
            continue
        last_ts = max(valid_ts)
        kept_tps = []
        for tp, ts in zip(tps, parsed):
            interaction_type = _tp_interaction_type(tp if isinstance(tp, dict) else {})
            if ts is None:
                # Keep touchpoints without timestamps; they're rare and carry some information
                kept_tps.append(tp)
                continue
            if interaction_type == "impression":
                if impression_days > 0 and ts >= (last_ts - timedelta(days=float(impression_days))):
                    kept_tps.append(tp)
            else:
                if click_days <= 0 or ts >= (last_ts - timedelta(days=float(click_days))):
                    kept_tps.append(tp)
        if interaction_mode == "click_only":
            kept_tps = [tp for tp in kept_tps if _tp_interaction_type(tp) in {"click", "visit", "direct"}]
        elif interaction_mode == "click_preferred":
            kept_tps = _dedupe_click_preferred_touchpoints(kept_tps)
        if not kept_tps:
            # If all touchpoints fall out of window, drop journey from attribution set
            continue
        new_j = dict(j)
        new_j["touchpoints"] = kept_tps
        path_kind = classify_journey_interaction(new_j)
        new_j.setdefault("meta", {})["interaction_summary"] = {
            "path_type": path_kind,
            "impression_touchpoints": sum(1 for tp in kept_tps if _tp_interaction_type(tp) == "impression"),
            "click_touchpoints": sum(1 for tp in kept_tps if _tp_interaction_type(tp) in {"click", "visit", "direct"}),
        }
        if path_kind == "view_through" and not include_view_only:
            continue
        out.append(new_j)

    return out


def annotate_journeys_with_conversion_key(
    journeys: List[Dict[str, Any]],
    config_json: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Attach kpi_type based on config. Supports v1 (converted) and v2 (conversions)."""
    conv_cfg = config_json.get("conversions") or {}
    primary_key = conv_cfg.get("primary_conversion_key")
    if not primary_key:
        return journeys

    out: List[Dict[str, Any]] = []
    for j in journeys:
        if j.get("kpi_type"):
            out.append(j)
            continue
        converted = j.get("converted")
        if converted is None:
            converted = bool(j.get("conversions"))
        new_j = dict(j)
        if converted:
            new_j["kpi_type"] = primary_key
        out.append(new_j)
    return out


def apply_model_config_to_journeys(
    journeys: List[Dict[str, Any]],
    config_json: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Apply both time windows and conversion-key annotations to journeys."""
    quality_cfg = config_json.get("quality") or {}
    tmp = filter_journeys_by_quality(
        journeys,
        min_quality_score=int(quality_cfg.get("min_journey_quality_score") or 0),
    )
    tmp = filter_journeys_by_windows(tmp, config_json)
    tmp = annotate_journeys_with_conversion_key(tmp, config_json)
    return tmp


# ---------------------------------------------------------------------------
# Normalised storage: ConversionPath helpers
# ---------------------------------------------------------------------------


def _journey_time_bounds(journey: Dict[str, Any]) -> Dict[str, Optional[datetime]]:
    """Derive first/last/conversion timestamps. Supports v1 and v2 (touchpoints.ts, conversions[0].ts)."""
    tps = journey.get("touchpoints") or []
    convs = journey.get("conversions") or []
    parsed = [_parse_ts(_tp_timestamp(tp)) for tp in tps]
    valid = [p.to_pydatetime() for p in parsed if p is not None]
    if not valid:
        return {"first": None, "last": None, "conversion": None}
    first = min(valid)
    last = max(valid)
    conv_ts = None
    if convs and isinstance(convs[0], dict):
        conv_ts = _parse_ts(convs[0].get("ts"))
        if conv_ts is not None:
            conv_ts = conv_ts.to_pydatetime()
    return {"first": first, "last": last, "conversion": conv_ts or last}


def _journey_path_hash(journey: Dict[str, Any]) -> str:
    """Stable hash for the ordered channel path of a journey."""
    tps = journey.get("touchpoints") or []
    channels = [str(tp.get("channel", "unknown")) for tp in tps]
    path_str = " > ".join(channels)
    h = hashlib.sha256(path_str.encode("utf-8"))
    return h.hexdigest()


def _is_v2_journey(j: Dict[str, Any]) -> bool:
    """Check if journey is internal v2 format."""
    return "_schema" in j or (j.get("customer") and isinstance(j["customer"], dict) and "id" in j.get("customer", {}))


def _journey_identity(journey: Dict[str, Any], *, idx: int) -> Dict[str, Optional[str]]:
    if _is_v2_journey(journey):
        customer = journey.get("customer") or {}
        profile_id = str(customer.get("id", f"anon-{idx}"))
        conversions = journey.get("conversions") or []
        return {
            "profile_id": profile_id,
            "conversion_id": str(journey.get("journey_id") or f"{profile_id}-{idx}"),
            "kpi_type": journey.get("kpi_type") or (conversions[0].get("name") if conversions else None),
        }
    profile_id = str(journey.get("customer_id") or journey.get("profile_id") or journey.get("id") or f"anon-{idx}")
    return {
        "profile_id": profile_id,
        "conversion_id": str(
            journey.get("conversion_id")
            or journey.get("order_id")
            or journey.get("transaction_id")
            or f"{profile_id}-{idx}"
        ),
        "kpi_type": journey.get("kpi_type"),
    }


def conversion_path_payload(row: Any) -> Dict[str, Any]:
    payload = getattr(row, "path_json", None)
    return payload if isinstance(payload, dict) else {}


def conversion_path_touchpoints(row: Any) -> List[Dict[str, Any]]:
    touchpoints = conversion_path_payload(row).get("touchpoints") or []
    if not isinstance(touchpoints, list):
        return []
    return [tp for tp in touchpoints if isinstance(tp, dict)]


def conversion_path_is_converted(row: Any) -> bool:
    conversion_key = getattr(row, "conversion_key", None)
    if isinstance(conversion_key, str) and conversion_key.strip():
        return True
    payload = conversion_path_payload(row)
    conversions = payload.get("conversions")
    if isinstance(conversions, list) and conversions:
        return True
    return bool(payload.get("converted"))


def conversion_path_outcome_summary(row: Any) -> Dict[str, Any]:
    return journey_outcome_summary(conversion_path_payload(row))


def conversion_path_revenue_value(
    row: Any,
    *,
    revenue_config: Optional[Dict[str, Any]] = None,
    dedupe_seen: Optional[set[str]] = None,
) -> float:
    payload = conversion_path_payload(row)
    value = compute_payload_revenue_value(
        payload,
        revenue_config or get_revenue_config(),
        dedupe_seen=dedupe_seen,
        fallback_conversion_id=str(getattr(row, "conversion_id", "") or ""),
    )
    if abs(value) > 1e-9:
        return float(value)
    fallback_raw = payload.get("value")
    if fallback_raw in (None, "", []):
        fallback_raw = payload.get("conversion_value")
    try:
        return float(fallback_raw or 0.0)
    except Exception:
        return 0.0


def persist_journeys_as_conversion_paths(
    db: Session,
    journeys: List[Dict[str, Any]],
    conversion_key: Optional[str] = None,
    replace: bool = True,
    replace_profile_ids: Optional[List[str]] = None,
    import_source: Optional[str] = None,
    import_batch_id: Optional[str] = None,
    source_snapshot_id: Optional[str] = None,
) -> int:
    """Persist journeys (v1 or internal v2) into ConversionPath. Stores path_json as-is."""
    effective_import_batch_id = str(import_batch_id or uuid.uuid4())
    normalized_replace_profile_ids = [
        str(profile_id).strip()
        for profile_id in (replace_profile_ids or [])
        if str(profile_id).strip()
    ]

    if replace:
        q = db.query(ConversionPath)
        if conversion_key is not None:
            q = q.filter(ConversionPath.conversion_key == conversion_key)
        if normalized_replace_profile_ids:
            q = q.filter(ConversionPath.profile_id.in_(normalized_replace_profile_ids))
        q.delete(synchronize_session=False)
        db.commit()
    else:
        candidate_conversion_ids = {
            str((_journey_identity(journey, idx=idx).get("conversion_id") or ""))
            for idx, journey in enumerate(journeys)
        }
        existing_query = db.query(ConversionPath.conversion_id)
        if conversion_key is not None:
            existing_query = existing_query.filter(ConversionPath.conversion_key == conversion_key)
        existing_conversion_ids = {
            str(row[0])
            for row in existing_query.filter(ConversionPath.conversion_id.in_(candidate_conversion_ids)).all()
        }
        journeys = [
            journey
            for idx, journey in enumerate(journeys)
            if str((_journey_identity(journey, idx=idx).get("conversion_id") or "")) not in existing_conversion_ids
        ]
    if not journeys:
        return 0

    now = datetime.utcnow()
    inserted = 0
    seen_conversion_ids = set()
    for idx, j in enumerate(journeys):
        identity = _journey_identity(j, idx=idx)
        profile_id = str(identity.get("profile_id") or f"anon-{idx}")
        conv_id = str(identity.get("conversion_id") or f"{profile_id}-{idx}")
        kpi_type = identity.get("kpi_type")
        effective_key = conversion_key or (kpi_type if isinstance(kpi_type, str) else None)
        if conv_id in seen_conversion_ids:
            continue
        seen_conversion_ids.add(conv_id)

        bounds = _journey_time_bounds(j)
        first_ts = bounds["first"] or now
        last_ts = bounds["last"] or first_ts
        conv_ts = bounds["conversion"] or last_ts

        length = len(j.get("touchpoints") or [])
        path_hash = _journey_path_hash(j)

        row = ConversionPath(
            conversion_id=conv_id,
            profile_id=profile_id,
            conversion_key=effective_key,
            conversion_ts=conv_ts,
            path_json=j,
            path_hash=path_hash,
            length=length,
            first_touch_ts=first_ts,
            last_touch_ts=last_ts,
            import_batch_id=effective_import_batch_id,
            import_source=(str(import_source).strip() or None) if import_source is not None else None,
            source_snapshot_id=(str(source_snapshot_id).strip() or None) if source_snapshot_id is not None else None,
            imported_at=now,
        )
        db.add(row)
        inserted += 1

    db.commit()
    return inserted


def v2_to_legacy(j: Dict[str, Any]) -> Dict[str, Any]:
    """Convert internal v2 journey to legacy format for attribution engine."""
    if not _is_v2_journey(j):
        return j
    cust = j.get("customer") or {}
    convs = j.get("conversions") or []
    primary = convs[0] if convs else {}
    tps = []
    for tp in j.get("touchpoints") or []:
        lt = {"channel": tp.get("channel", "unknown")}
        ts = tp.get("ts") or tp.get("timestamp")
        if ts:
            lt["timestamp"] = ts
        lt["interaction_type"] = _tp_interaction_type(tp)
        source = tp.get("source")
        medium = tp.get("medium")
        if source:
            lt["source"] = source
            lt["utm_source"] = source
        if medium:
            lt["medium"] = medium
            lt["utm_medium"] = medium
        camp = tp.get("campaign")
        if camp:
            lt["campaign"] = camp.get("name", camp) if isinstance(camp, dict) else camp
            lt["utm_campaign"] = lt["campaign"]
        utm = tp.get("utm")
        if isinstance(utm, dict):
            lt["utm"] = {
                "source": utm.get("source") or lt.get("utm_source"),
                "medium": utm.get("medium") or lt.get("utm_medium"),
                "campaign": utm.get("campaign") or lt.get("utm_campaign"),
            }
            if utm.get("source"):
                lt["utm_source"] = utm.get("source")
            if utm.get("medium"):
                lt["utm_medium"] = utm.get("medium")
            if utm.get("campaign"):
                lt["utm_campaign"] = utm.get("campaign")
        for field in ("impression_id", "click_id", "placement_id", "creative_id", "ad_id", "campaign_id"):
            if tp.get(field) not in (None, "", []):
                lt[field] = tp.get(field)
        tps.append(lt)
    outcome = journey_outcome_summary(j)
    return {
        "customer_id": cust.get("id", "unknown"),
        "touchpoints": tps,
        "converted": len(convs) > 0,
        "conversion_value": float(primary.get("value", 0)) if primary else 0.0,
        "kpi_type": primary.get("name") if primary else j.get("kpi_type"),
        "meta": j.get("meta") if isinstance(j.get("meta"), dict) else {},
        "quality_score": journey_quality_score(j),
        "quality_band": (
            (((j.get("meta") or {}).get("quality") or {}).get("band"))
            if isinstance(j.get("meta"), dict)
            else None
        ),
        "conversion_outcome": outcome,
        "interaction_path_type": classify_journey_interaction({"touchpoints": tps}),
    }


def load_journeys_from_db(
    db: Session,
    conversion_key: Optional[str] = None,
    limit: int = 50000,
) -> List[Dict[str, Any]]:
    """Load journeys from DB. Converts v2 to legacy format for attribution."""
    q = db.query(ConversionPath)
    if conversion_key is not None:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    rows = q.order_by(ConversionPath.conversion_ts.desc()).limit(limit).all()

    revenue_config = get_revenue_config()
    dedupe_seen = set()
    journeys: List[Dict[str, Any]] = []
    for r in rows:
        payload = conversion_path_payload(r)
        if payload:
            legacy = v2_to_legacy(payload)
            entries = extract_revenue_entries(
                payload,
                revenue_config,
                fallback_conversion_id=str(getattr(r, "conversion_id", "") or ""),
            )
            revenue_value = conversion_path_revenue_value(
                r,
                revenue_config=revenue_config,
                dedupe_seen=dedupe_seen,
            )
            legacy["conversion_value"] = float(revenue_value)
            legacy["_revenue_entries"] = entries
            legacy["conversion_outcome"] = conversion_path_outcome_summary(r)
            journeys.append(legacy)
    return journeys
