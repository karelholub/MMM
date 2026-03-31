from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from app.models_config_dq import ConversionPath, ConversionScopeDiagnosticFact
from app.services_conversions import (
    conversion_path_is_converted,
    conversion_path_payload,
    conversion_path_revenue_value,
    conversion_path_touchpoints,
)
from app.services_journey_aggregates import (
    STEP_ADD_TO_CART,
    STEP_CHECKOUT,
    STEP_CONTENT_VIEW,
    map_touchpoint_step,
)


def _to_utc_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_day_start(date_from: str) -> datetime:
    dt = _to_utc_dt(f"{date_from[:10]}T00:00:00+00:00")
    assert dt is not None
    return dt


def _parse_day_end(date_to: str) -> datetime:
    dt = _to_utc_dt(f"{date_to[:10]}T23:59:59.999999+00:00")
    assert dt is not None
    return dt


def _touchpoint_ts(tp: Dict[str, Any]) -> Optional[datetime]:
    for key in ("ts", "timestamp", "event_ts", "occurred_at", "time"):
        dt = _to_utc_dt(tp.get(key))
        if dt is not None:
            return dt
    return None


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
    channel = str(tp.get("channel") or "unknown")
    if scope_type == "campaign":
        campaign_name = _campaign_name(tp)
        return f"{channel}:{campaign_name}" if campaign_name else channel
    return channel


def _empty_diag() -> Dict[str, Any]:
    return {
        "roles": {
            "first_touch_conversions": 0,
            "last_touch_conversions": 0,
            "assist_conversions": 0,
            "first_touch_revenue": 0.0,
            "last_touch_revenue": 0.0,
            "assist_revenue": 0.0,
        },
        "funnel": {
            "touch_journeys": 0,
            "content_journeys": 0,
            "checkout_journeys": 0,
            "converted_journeys": 0,
        },
    }


def _finalize_diag(diag: Dict[str, Any]) -> None:
    roles = diag["roles"]
    funnel = diag["funnel"]
    for key in ("first_touch_revenue", "last_touch_revenue", "assist_revenue"):
        roles[key] = round(float(roles[key]), 2)
    touch = max(float(funnel["touch_journeys"]), 1.0)
    funnel["content_rate"] = round(float(funnel["content_journeys"]) / touch, 4)
    funnel["checkout_rate"] = round(float(funnel["checkout_journeys"]) / touch, 4)
    funnel["conversion_rate"] = round(float(funnel["converted_journeys"]) / touch, 4)


def _iter_scope_conversion_rows(
    *,
    db: Session,
    start: datetime,
    end: datetime,
    conversion_key: Optional[str],
):
    q = db.query(
        ConversionPath.conversion_id,
        ConversionPath.conversion_key,
        ConversionPath.conversion_ts,
        ConversionPath.first_touch_ts,
        ConversionPath.last_touch_ts,
        ConversionPath.path_json,
    ).filter(
        ConversionPath.last_touch_ts >= start,
        ConversionPath.first_touch_ts <= end,
    )
    if conversion_key:
        q = q.filter(ConversionPath.conversion_key == conversion_key)
    for row in q.yield_per(1000):
        yield SimpleNamespace(
            conversion_id=row[0],
            conversion_key=row[1],
            conversion_ts=row[2],
            first_touch_ts=row[3],
            last_touch_ts=row[4],
            path_json=row[5] if isinstance(row[5], dict) else {},
        )


def _aggregate_scope_diagnostics_from_facts(
    *,
    db: Session,
    scope_type: str,
    start: datetime,
    end: datetime,
    conversion_key: Optional[str],
    channels: Optional[List[str]],
) -> Optional[Dict[str, Dict[str, Any]]]:
    allowed_channels = set(channels or [])
    q = db.query(ConversionScopeDiagnosticFact).filter(
        ConversionScopeDiagnosticFact.scope_type == scope_type,
        ConversionScopeDiagnosticFact.last_touch_ts >= start,
        ConversionScopeDiagnosticFact.first_touch_ts <= end,
    )
    if conversion_key:
        q = q.filter(ConversionScopeDiagnosticFact.conversion_key == conversion_key)
    if allowed_channels:
        q = q.filter(ConversionScopeDiagnosticFact.scope_channel.in_(allowed_channels))
    fact_rows = q.all()
    if not fact_rows:
        return None

    if allowed_channels:
        raw_count = 0
        for row in _iter_scope_conversion_rows(
            db=db,
            start=start,
            end=end,
            conversion_key=conversion_key,
        ):
            if any(str(tp.get("channel") or "unknown") in allowed_channels for tp in conversion_path_touchpoints(row)):
                raw_count += 1
    else:
        raw_count_query = db.query(ConversionPath.conversion_id).filter(
            ConversionPath.last_touch_ts >= start,
            ConversionPath.first_touch_ts <= end,
        )
        if conversion_key:
            raw_count_query = raw_count_query.filter(ConversionPath.conversion_key == conversion_key)
        raw_count = raw_count_query.count()
    fact_count = len({str(row.conversion_id) for row in fact_rows})
    if raw_count > fact_count:
        return None

    out: Dict[str, Dict[str, Any]] = {}
    for row in fact_rows:
        diag = out.setdefault(str(row.scope_key), _empty_diag())
        roles = diag["roles"]
        funnel = diag["funnel"]
        roles["first_touch_conversions"] += int(row.first_touch_conversions or 0)
        roles["last_touch_conversions"] += int(row.last_touch_conversions or 0)
        roles["assist_conversions"] += int(row.assist_conversions or 0)
        roles["first_touch_revenue"] += float(row.first_touch_revenue or 0.0)
        roles["last_touch_revenue"] += float(row.last_touch_revenue or 0.0)
        roles["assist_revenue"] += float(row.assist_revenue or 0.0)
        funnel["touch_journeys"] += int(row.touch_journeys or 0)
        funnel["content_journeys"] += int(row.content_journeys or 0)
        funnel["checkout_journeys"] += int(row.checkout_journeys or 0)
        funnel["converted_journeys"] += int(row.converted_journeys or 0)

    for diag in out.values():
        _finalize_diag(diag)
    return out


def build_scope_diagnostics(
    *,
    db: Session,
    scope_type: str,
    date_from: str,
    date_to: str,
    conversion_key: Optional[str] = None,
    channels: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    if scope_type not in {"channel", "campaign"}:
        raise ValueError("scope_type must be channel or campaign")
    start = _parse_day_start(date_from)
    end = _parse_day_end(date_to)
    fact_result = _aggregate_scope_diagnostics_from_facts(
        db=db,
        scope_type=scope_type,
        start=start,
        end=end,
        conversion_key=conversion_key,
        channels=channels,
    )
    if fact_result is not None:
        return fact_result
    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)

    out: Dict[str, Dict[str, Any]] = {}
    dedupe_seen: set[str] = set()

    for row in _iter_scope_conversion_rows(
        db=db,
        start=start,
        end=end,
        conversion_key=conversion_key,
    ):
        payload = conversion_path_payload(row)
        touchpoints = conversion_path_touchpoints(row)
        if not touchpoints:
            continue

        dims_any: Set[str] = set()
        dims_content: Set[str] = set()
        dims_checkout: Set[str] = set()
        ordered_dims: List[str] = []
        for idx, tp in enumerate(touchpoints):
            channel = str(tp.get("channel") or "unknown")
            if filter_channels and channel not in allowed_channels:
                continue
            dim = _dimension_key(tp, scope_type)
            dims_any.add(dim)
            ordered_dims.append(dim)
            step = map_touchpoint_step(tp, idx)
            if step in {STEP_CONTENT_VIEW, STEP_ADD_TO_CART}:
                dims_content.add(dim)
            if step == STEP_CHECKOUT:
                dims_checkout.add(dim)

        if not dims_any:
            continue

        for dim in dims_any:
            diag = out.setdefault(dim, _empty_diag())
            diag["funnel"]["touch_journeys"] += 1
        for dim in dims_content:
            diag = out.setdefault(dim, _empty_diag())
            diag["funnel"]["content_journeys"] += 1
        for dim in dims_checkout:
            diag = out.setdefault(dim, _empty_diag())
            diag["funnel"]["checkout_journeys"] += 1

        converted = conversion_path_is_converted(row)
        conv_ts = _to_utc_dt(getattr(row, "conversion_ts", None))
        if converted and conv_ts is not None and start <= conv_ts <= end:
            value = conversion_path_revenue_value(row, dedupe_seen=dedupe_seen)
            for dim in dims_any:
                diag = out.setdefault(dim, _empty_diag())
                diag["funnel"]["converted_journeys"] += 1

            first_dim = ordered_dims[0] if ordered_dims else None
            last_dim = ordered_dims[-1] if ordered_dims else None
            if first_dim:
                diag = out.setdefault(first_dim, _empty_diag())
                diag["roles"]["first_touch_conversions"] += 1
                diag["roles"]["first_touch_revenue"] += value
            if last_dim:
                diag = out.setdefault(last_dim, _empty_diag())
                diag["roles"]["last_touch_conversions"] += 1
                diag["roles"]["last_touch_revenue"] += value
            assist_dims = list(dict.fromkeys(ordered_dims[1:-1]))
            if assist_dims:
                share = value / float(len(assist_dims))
                for dim in assist_dims:
                    diag = out.setdefault(dim, _empty_diag())
                    diag["roles"]["assist_conversions"] += 1
                    diag["roles"]["assist_revenue"] += share

    for diag in out.values():
        _finalize_diag(diag)
    return out
