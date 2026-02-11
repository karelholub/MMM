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

import pandas as pd
from sqlalchemy.orm import Session

from .models_config_dq import ConversionPath


def _parse_ts(ts: Any):
    """Best-effort timestamp parsing. Returns pandas.Timestamp or None."""
    if not ts:
        return None
    try:
        return pd.to_datetime(ts, errors="coerce")
    except Exception:
        return None


def filter_journeys_by_windows(
    journeys: List[Dict[str, Any]],
    config_json: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Apply time windows from config.windows to touchpoints.

    Limitations:
      - We treat all touchpoints as "click-like" and apply click_lookback_days.
      - Impression windows would require event-type granularity that current paths lack.
    """
    windows = config_json.get("windows") or {}
    click_days = windows.get("click_lookback_days")
    if not click_days or click_days <= 0:
        return journeys

    max_delta = timedelta(days=float(click_days))
    out: List[Dict[str, Any]] = []

    for j in journeys:
        tps = j.get("touchpoints") or []
        if not tps:
            continue
        parsed = [_parse_ts(tp.get("timestamp")) for tp in tps]
        valid_ts = [p for p in parsed if p is not None]
        if not valid_ts:
            # No usable timestamps; keep journey as-is to avoid dropping data silently
            out.append(j)
            continue
        last_ts = max(valid_ts)
        min_ts = last_ts - max_delta
        kept_tps = []
        for tp, ts in zip(tps, parsed):
            if ts is None:
                # Keep touchpoints without timestamps; they're rare and carry some information
                kept_tps.append(tp)
            elif ts >= min_ts:
                kept_tps.append(tp)
        if not kept_tps:
            # If all touchpoints fall out of window, drop journey from attribution set
            continue
        new_j = dict(j)
        new_j["touchpoints"] = kept_tps
        out.append(new_j)

    return out


def annotate_journeys_with_conversion_key(
    journeys: List[Dict[str, Any]],
    config_json: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Attach kpi_type to journeys based on config.conversions.primary_conversion_key.

    If a journey already has kpi_type (e.g. from sample data), it is preserved.
    """
    conv_cfg = config_json.get("conversions") or {}
    primary_key = conv_cfg.get("primary_conversion_key")
    if not primary_key:
        return journeys

    out: List[Dict[str, Any]] = []
    for j in journeys:
        if j.get("kpi_type"):
            out.append(j)
            continue
        converted = j.get("converted", True)
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
    tmp = filter_journeys_by_windows(journeys, config_json)
    tmp = annotate_journeys_with_conversion_key(tmp, config_json)
    return tmp


# ---------------------------------------------------------------------------
# Normalised storage: ConversionPath helpers
# ---------------------------------------------------------------------------


def _journey_time_bounds(journey: Dict[str, Any]) -> Dict[str, Optional[datetime]]:
    """Derive first/last/conversion timestamps from a journey's touchpoints."""
    tps = journey.get("touchpoints") or []
    if not tps:
        return {"first": None, "last": None, "conversion": None}

    parsed = [_parse_ts(tp.get("timestamp")) for tp in tps]
    valid = [p.to_pydatetime() for p in parsed if p is not None]
    if not valid:
        return {"first": None, "last": None, "conversion": None}

    first = min(valid)
    last = max(valid)
    return {"first": first, "last": last, "conversion": last}


def _journey_path_hash(journey: Dict[str, Any]) -> str:
    """Stable hash for the ordered channel path of a journey."""
    tps = journey.get("touchpoints") or []
    channels = [str(tp.get("channel", "unknown")) for tp in tps]
    path_str = " > ".join(channels)
    h = hashlib.sha256(path_str.encode("utf-8"))
    return h.hexdigest()


def persist_journeys_as_conversion_paths(
    db: Session,
    journeys: List[Dict[str, Any]],
    conversion_key: Optional[str] = None,
    replace: bool = True,
) -> int:
    """Persist journeys into the normalised ConversionPath table.

    - Stores the full journey dict in ConversionPath.path_json
    - Derives first/last/conversion timestamps from touchpoints
    - Computes a stable hash of the channel path for aggregation
    """
    if not journeys:
        return 0

    # Simple replace strategy for now: either per-conversion_key or global.
    if replace:
        q = db.query(ConversionPath)
        if conversion_key is not None:
            q = q.filter(ConversionPath.conversion_key == conversion_key)
        q.delete(synchronize_session=False)

    now = datetime.utcnow()
    inserted = 0
    for idx, j in enumerate(journeys):
        # Basic identifiers
        profile_id = str(j.get("customer_id") or j.get("profile_id") or j.get("id") or f"anon-{idx}")
        conv_id = str(
            j.get("conversion_id")
            or j.get("order_id")
            or j.get("transaction_id")
            or f"{profile_id}-{idx}"
        )
        kpi_type = j.get("kpi_type")
        effective_key = conversion_key or (kpi_type if isinstance(kpi_type, str) else None)

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
        )
        db.add(row)
        inserted += 1

    db.commit()
    return inserted


def load_journeys_from_db(
    db: Session,
    conversion_key: Optional[str] = None,
    limit: int = 50000,
) -> List[Dict[str, Any]]:
    """Load journeys from the ConversionPath table, returning journey dicts.

    This provides a bridge between the normalised storage and in-memory
    attribution APIs that still expect journey dictionaries.
    """
    q = db.query(ConversionPath)
    if conversion_key is not None:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    rows = (
        q.order_by(ConversionPath.conversion_ts.asc())
        .limit(limit)
        .all()
    )

    journeys: List[Dict[str, Any]] = []
    for r in rows:
        payload = r.path_json
        if isinstance(payload, dict):
            journeys.append(payload)
    return journeys


