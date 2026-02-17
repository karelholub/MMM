from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from app.services_metrics import journey_revenue_value


def compute_journey_validation(journeys: List[Dict[str, Any]]) -> Dict[str, Any]:
    seen_ids: Dict[str, int] = {}
    error_list: List[str] = []
    warn_list: List[str] = []
    for journey in journeys or []:
        customer_id = str(journey.get("customer_id") or "")
        if customer_id:
            seen_ids[customer_id] = seen_ids.get(customer_id, 0) + 1
        touchpoints = journey.get("touchpoints") or []
        if not touchpoints:
            error_list.append("Journey has no touchpoints")
        for tp in touchpoints:
            if not tp.get("channel") and not tp.get("source"):
                warn_list.append("Touchpoint missing channel/source")
            if not tp.get("timestamp"):
                warn_list.append("Touchpoint missing timestamp")
    dup_ids = [pid for pid, c in seen_ids.items() if c > 1]
    if dup_ids:
        warn_list.append(f"Duplicate customer_ids: {len(dup_ids)} ids")
    top_errors = list(dict.fromkeys(error_list))[:5]
    top_warnings = list(dict.fromkeys(warn_list))[:5]
    return {
        "error_count": len(error_list),
        "warn_count": len(warn_list),
        "top_errors": top_errors,
        "top_warnings": top_warnings,
        "duplicate_ids_count": len(dup_ids),
    }


def compute_data_freshness_hours(last_ts: Any) -> Optional[float]:
    if last_ts is None:
        return None
    try:
        dt = pd.to_datetime(last_ts, errors="coerce") if isinstance(last_ts, str) else last_ts
        if dt is None or pd.isna(dt):
            return None
        delta = datetime.utcnow() - (dt.to_pydatetime() if hasattr(dt, "to_pydatetime") else dt)
        return delta.total_seconds() / 3600.0
    except Exception:
        return None


def derive_system_state(
    loaded: bool,
    count: int,
    last_import_at: Optional[str],
    freshness_hours: Optional[float],
    error_count: int,
    warn_count: int,
) -> str:
    if not loaded or count == 0:
        return "empty"
    if error_count > 0:
        return "error"
    if warn_count > 0 and warn_count >= count * 0.1:
        return "partial"
    if freshness_hours is not None and freshness_hours > 168:
        return "stale"
    return "data_loaded"


def build_journeys_summary(
    *,
    journeys: List[Dict[str, Any]],
    kpi_config: Any,
    get_import_runs_fn: Callable[..., List[Dict[str, Any]]],
) -> Dict[str, Any]:
    converted = [j for j in journeys if j.get("converted", True)]
    total_value = 0.0
    dedupe_seen_total: set[str] = set()
    for journey in converted:
        total_value += journey_revenue_value(journey, dedupe_seen=dedupe_seen_total)

    channels: set[str] = set()
    first_ts = None
    last_ts = None
    for journey in journeys:
        for tp in journey.get("touchpoints", []):
            channels.add(tp.get("channel", "unknown"))
            ts = tp.get("timestamp")
            if not ts:
                continue
            try:
                dt = pd.to_datetime(ts, errors="coerce")
            except Exception:
                dt = None
            if dt is None or pd.isna(dt):
                continue
            if first_ts is None or dt < first_ts:
                first_ts = dt
            if last_ts is None or dt > last_ts:
                last_ts = dt

    kpi_counts: Dict[str, int] = {}
    for journey in journeys:
        ktype = journey.get("kpi_type")
        if isinstance(ktype, str):
            kpi_counts[ktype] = kpi_counts.get(ktype, 0) + 1

    primary_kpi_id = kpi_config.primary_kpi_id
    primary_kpi_label = None
    if primary_kpi_id:
        for definition in kpi_config.definitions:
            if definition.id == primary_kpi_id:
                primary_kpi_label = definition.label
                break
    primary_count = kpi_counts.get(primary_kpi_id, len(converted) if primary_kpi_id else len(converted))

    runs = get_import_runs_fn(limit=50)
    last_run = next((r for r in runs if r.get("status") == "success"), None)
    last_import_at = last_run.get("at") if last_run else None
    last_import_source = last_run.get("source") if last_run else None
    freshness_hours = compute_data_freshness_hours(last_ts)
    validation = compute_journey_validation(journeys)
    system_state = derive_system_state(
        True,
        len(journeys),
        last_import_at,
        freshness_hours,
        validation["error_count"],
        validation["warn_count"],
    )

    return {
        "loaded": True,
        "count": len(journeys),
        "converted": len(converted),
        "non_converted": len(journeys) - len(converted),
        "channels": sorted(channels),
        "total_value": total_value,
        "primary_kpi_id": primary_kpi_id,
        "primary_kpi_label": primary_kpi_label,
        "primary_kpi_count": primary_count,
        "kpi_counts": kpi_counts,
        "date_min": first_ts.isoformat() if first_ts is not None else None,
        "date_max": last_ts.isoformat() if last_ts is not None else None,
        "last_import_at": last_import_at,
        "last_import_source": last_import_source,
        "data_freshness_hours": round(freshness_hours, 1) if freshness_hours is not None else None,
        "system_state": system_state,
        "validation": validation,
    }


def build_journeys_preview(*, journeys: List[Dict[str, Any]], limit: int) -> Dict[str, Any]:
    rows = []
    for idx, journey in enumerate(journeys[:limit]):
        touchpoints = journey.get("touchpoints") or []
        first_ts = None
        last_ts = None
        for tp in touchpoints:
            ts = tp.get("timestamp")
            if ts:
                try:
                    dt = pd.to_datetime(ts, errors="coerce")
                    if dt is not None and not pd.isna(dt):
                        if first_ts is None or dt < first_ts:
                            first_ts = dt
                        if last_ts is None or dt > last_ts:
                            last_ts = dt
                except Exception:
                    pass
        channels = list(dict.fromkeys(tp.get("channel", "?") for tp in touchpoints))
        rows.append(
            {
                "validIndex": idx,
                "customer_id": journey.get("customer_id") or journey.get("profile_id") or journey.get("id") or "—",
                "touchpoints_count": len(touchpoints),
                "first_ts": first_ts.isoformat() if first_ts is not None else None,
                "last_ts": last_ts.isoformat() if last_ts is not None else None,
                "converted": journey.get("converted", True),
                "conversion_value": journey.get("conversion_value"),
                "revenue_value": journey_revenue_value(journey),
                "channels_list": channels,
            }
        )
    return {
        "rows": rows,
        "columns": [
            "customer_id",
            "touchpoints_count",
            "first_ts",
            "last_ts",
            "converted",
            "conversion_value",
            "revenue_value",
            "channels_list",
        ],
        "total": len(journeys),
    }
