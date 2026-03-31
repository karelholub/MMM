from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .models_config_dq import SilverConversionFact, SilverTouchpointFact
from .services_metrics import journey_outcome_summary


def _to_utc_dt(value: Any) -> Optional[datetime]:
    if value in (None, "", []):
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


def _normalized_token(value: Any) -> Optional[str]:
    if value in (None, "", []):
        return None
    return str(value).strip() or None


def _source_token(tp: Dict[str, Any]) -> Optional[str]:
    utm = tp.get("utm") if isinstance(tp.get("utm"), dict) else {}
    return _normalized_token(tp.get("utm_source") or utm.get("source") or tp.get("source"))


def _medium_token(tp: Dict[str, Any]) -> Optional[str]:
    utm = tp.get("utm") if isinstance(tp.get("utm"), dict) else {}
    return _normalized_token(tp.get("utm_medium") or utm.get("medium") or tp.get("medium"))


def _campaign_token(tp: Dict[str, Any]) -> Optional[str]:
    utm = tp.get("utm") if isinstance(tp.get("utm"), dict) else {}
    campaign = tp.get("utm_campaign") or utm.get("campaign") or tp.get("campaign") or tp.get("campaign_name")
    if isinstance(campaign, dict):
        campaign = campaign.get("name") or campaign.get("id")
    return _normalized_token(campaign)


def _event_name(tp: Dict[str, Any]) -> Optional[str]:
    return _normalized_token(tp.get("event_name") or tp.get("event") or tp.get("name") or tp.get("type"))


def _interaction_path_type(journey: Dict[str, Any]) -> Optional[str]:
    explicit = (
        (((journey.get("meta") or {}).get("interaction_summary") or {}).get("path_type"))
        or journey.get("interaction_path_type")
    )
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    has_impression = False
    has_click_like = False
    for tp in journey.get("touchpoints") or []:
        if not isinstance(tp, dict):
            continue
        interaction = str(tp.get("interaction_type") or "").strip().lower()
        if interaction == "impression":
            has_impression = True
        elif interaction in {"click", "visit", "direct"}:
            has_click_like = True
    if has_impression and has_click_like:
        return "mixed_path"
    if has_click_like:
        return "click_through"
    if has_impression:
        return "view_through"
    return "unknown"


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _safe_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _safe_adjustment_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    mapping = {
        "refund": "refund",
        "partial_refund": "refund",
        "cancellation": "cancellation",
        "cancelled": "cancellation",
        "returned_order": "cancellation",
        "return": "cancellation",
        "invalid_lead": "invalid",
        "invalid": "invalid",
        "disqualified_lead": "disqualified",
        "disqualified": "disqualified",
        "duplicate_lead": "invalid",
        "spam_lead": "invalid",
        "test_lead": "invalid",
    }
    return mapping.get(raw, raw)


def _silver_outcome_summary(journey: Dict[str, Any], *, conversion_id: str) -> Dict[str, float]:
    payload = dict(journey)
    conversions = payload.get("conversions")
    if not isinstance(conversions, list) or not conversions:
        fallback = journey_outcome_summary(payload)
        if abs(float(fallback.get("gross_value", 0.0) or 0.0)) > 1e-9 or abs(float(fallback.get("gross_conversions", 0.0) or 0.0)) > 1e-9:
            return fallback
        gross = _safe_float(payload.get("conversion_value"))
        converted = 1.0 if gross > 0 else 0.0
        return {
            "gross_value": gross,
            "net_value": gross,
            "refunded_value": 0.0,
            "cancelled_value": 0.0,
            "gross_conversions": converted,
            "net_conversions": converted,
            "invalid_leads": 0.0,
            "valid_leads": converted,
        }

    summary = {
        "gross_value": 0.0,
        "net_value": 0.0,
        "refunded_value": 0.0,
        "cancelled_value": 0.0,
        "gross_conversions": 0.0,
        "net_conversions": 0.0,
        "invalid_leads": 0.0,
        "valid_leads": 0.0,
    }
    for conversion in conversions:
        if not isinstance(conversion, dict):
            continue
        gross = _safe_float(conversion.get("value") if conversion.get("value") not in (None, "", []) else payload.get("conversion_value"))
        refunded_value = 0.0
        cancelled_value = 0.0
        invalidated = False
        status = _safe_status(conversion.get("status"))
        for adjustment in conversion.get("adjustments") or []:
            if not isinstance(adjustment, dict):
                continue
            adjustment_value = _safe_float(adjustment.get("value"))
            adjustment_type = _safe_adjustment_type(adjustment.get("type"))
            if adjustment_type == "refund":
                refunded_value += adjustment_value
            elif adjustment_type == "cancellation":
                cancelled_value += gross
                status = "cancelled"
            elif adjustment_type in {"invalid", "disqualified"}:
                invalidated = True
                status = "disqualified" if adjustment_type == "disqualified" else "invalid"
        net = max(0.0, gross - refunded_value)
        if status in {"refunded", "cancelled"}:
            net = 0.0
        if status in {"invalid", "disqualified"} or invalidated:
            net = 0.0
        summary["gross_value"] += gross
        summary["net_value"] += net
        summary["refunded_value"] += refunded_value
        summary["cancelled_value"] += cancelled_value
        summary["gross_conversions"] += 1.0
        if status not in {"refunded", "cancelled", "invalid", "disqualified"} and not invalidated:
            summary["net_conversions"] += 1.0
            summary["valid_leads"] += 1.0
        elif status in {"invalid", "disqualified"} or invalidated:
            summary["invalid_leads"] += 1.0
    if abs(summary["gross_conversions"]) <= 1e-9 and conversion_id:
        summary["gross_conversions"] = 1.0
        summary["net_conversions"] = 1.0
        summary["valid_leads"] = 1.0
    return summary


def build_silver_conversion_fact(
    *,
    journey: Dict[str, Any],
    conversion_id: str,
    profile_id: str,
    conversion_key: Optional[str],
    conversion_ts: datetime,
    path_hash: str,
    path_length: int,
    import_batch_id: Optional[str],
    import_source: Optional[str],
    source_snapshot_id: Optional[str],
    created_at: datetime,
) -> SilverConversionFact:
    outcome = _silver_outcome_summary(journey, conversion_id=conversion_id)
    normalized_profile_id = str(profile_id or "").strip() or None
    interaction_path_type = _interaction_path_type(journey)
    return SilverConversionFact(
        conversion_id=conversion_id,
        profile_id=normalized_profile_id,
        conversion_key=conversion_key,
        conversion_ts=conversion_ts,
        import_batch_id=_normalized_token(import_batch_id),
        import_source=_normalized_token(import_source),
        source_snapshot_id=_normalized_token(source_snapshot_id),
        path_hash=path_hash or None,
        path_length=int(path_length or 0),
        interaction_path_type=interaction_path_type,
        gross_conversions_total=float(outcome.get("gross_conversions", 0.0) or 0.0),
        net_conversions_total=float(outcome.get("net_conversions", 0.0) or 0.0),
        gross_revenue_total=float(outcome.get("gross_value", 0.0) or 0.0),
        net_revenue_total=float(outcome.get("net_value", 0.0) or 0.0),
        refunded_value=float(outcome.get("refunded_value", 0.0) or 0.0),
        cancelled_value=float(outcome.get("cancelled_value", 0.0) or 0.0),
        invalid_leads=float(outcome.get("invalid_leads", 0.0) or 0.0),
        valid_leads=float(outcome.get("valid_leads", 0.0) or 0.0),
        device=_normalized_token(journey.get("device")),
        country=_normalized_token(journey.get("country")),
        created_at=created_at,
        updated_at=created_at,
    )


def build_silver_touchpoint_facts(
    *,
    journey: Dict[str, Any],
    conversion_id: str,
    profile_id: str,
    conversion_key: Optional[str],
    conversion_ts: datetime,
    import_batch_id: Optional[str],
    import_source: Optional[str],
    source_snapshot_id: Optional[str],
    created_at: datetime,
) -> List[SilverTouchpointFact]:
    normalized_profile_id = str(profile_id or "").strip() or None
    rows: List[SilverTouchpointFact] = []
    for ordinal, tp in enumerate(journey.get("touchpoints") or []):
        if not isinstance(tp, dict):
            continue
        rows.append(
            SilverTouchpointFact(
                conversion_id=conversion_id,
                profile_id=normalized_profile_id,
                conversion_key=conversion_key,
                conversion_ts=conversion_ts,
                ordinal=ordinal,
                touchpoint_ts=_to_utc_dt(tp.get("ts") or tp.get("timestamp") or tp.get("event_ts")),
                channel=_normalized_token(tp.get("channel")),
                source=_source_token(tp),
                medium=_medium_token(tp),
                campaign=_campaign_token(tp),
                event_name=_event_name(tp),
                interaction_type=_normalized_token(tp.get("interaction_type")),
                import_batch_id=_normalized_token(import_batch_id),
                import_source=_normalized_token(import_source),
                source_snapshot_id=_normalized_token(source_snapshot_id),
                created_at=created_at,
                updated_at=created_at,
            )
        )
    return rows
