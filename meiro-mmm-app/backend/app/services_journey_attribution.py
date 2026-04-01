"""Journey attribution summary adapter for paths/flow UIs.

Uses existing attribution models from app.attribution_engine and samples journey
instances from conversion_paths with journey-definition filters.
"""

from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy.orm import Session

from .attribution_engine import ATTRIBUTION_MODELS, run_attribution, run_attribution_campaign
from .models_config_dq import ConversionPath, JourneyDefinition
from .services_conversions import conversion_path_payload, v2_to_legacy
from .services_journey_aggregates import _build_journey_steps, _path_hash
from .services_silver_journeys import load_silver_journeys

PAID_CHANNEL_TOKENS = {
    "google_ads",
    "meta_ads",
    "linkedin_ads",
    "bing_ads",
    "tiktok_ads",
    "snapchat_ads",
    "paid",
    "cpc",
    "ppc",
    "paid_social",
    "display",
    "affiliate",
}


def _is_paid_channel(channel: str) -> bool:
    c = (channel or "").strip().lower().replace("-", "_").replace(" ", "_")
    return c in PAID_CHANNEL_TOKENS


def _channel_group(channel: str) -> str:
    return "paid" if _is_paid_channel(channel) else "organic"


def _model_kwargs(model: str, settings_obj: Any) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {}
    if model == "time_decay":
        kwargs["half_life_days"] = float(getattr(settings_obj.attribution, "time_decay_half_life_days", 7.0) or 7.0)
    elif model == "position_based":
        kwargs["first_pct"] = float(getattr(settings_obj.attribution, "position_first_pct", 0.4) or 0.4)
        kwargs["last_pct"] = float(getattr(settings_obj.attribution, "position_last_pct", 0.4) or 0.4)
    return kwargs


def _parse_date(value: str) -> date:
    return datetime.fromisoformat(value[:10]).date()


def _filter_conversion_paths(
    db: Session,
    *,
    definition: JourneyDefinition,
    date_from: date,
    date_to: date,
    channel_group: Optional[str],
    campaign_id: Optional[str],
    device: Optional[str],
    country: Optional[str],
    path_hash: Optional[str],
) -> List[Tuple[Any, Dict[str, Any], Dict[str, Optional[str]], str]]:
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    q = db.query(
        ConversionPath.conversion_id,
        ConversionPath.conversion_key,
        ConversionPath.conversion_ts,
        ConversionPath.path_json,
    ).filter(ConversionPath.conversion_ts >= start_dt, ConversionPath.conversion_ts < end_dt)
    if definition.conversion_kpi_id:
        q = q.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)
    rows = q.all()

    out: List[Tuple[Any, Dict[str, Any], Dict[str, Optional[str]], str]] = []
    for row in rows:
        light_row = SimpleNamespace(
            conversion_id=row[0],
            conversion_key=row[1],
            conversion_ts=row[2],
            path_json=row[3] if isinstance(row[3], dict) else {},
        )
        payload = conversion_path_payload(light_row)
        conv_ts = light_row.conversion_ts
        if conv_ts.tzinfo is None:
            conv_ts = conv_ts.replace(tzinfo=timezone.utc)
        steps, _, dims = _build_journey_steps(
            payload,
            conversion_ts=conv_ts,
            lookback_window_days=definition.lookback_window_days,
        )
        ph = _path_hash(steps) if steps else ""
        if path_hash and ph != path_hash:
            continue
        if channel_group and (dims.get("channel_group") or "").lower() != channel_group.lower():
            continue
        if campaign_id and (dims.get("campaign_id") or "") != campaign_id:
            continue
        if device and (dims.get("device") or "").lower() != device.lower():
            continue
        if country and (dims.get("country") or "").lower() != country.lower():
            continue
        out.append((light_row, payload, dims, ph))
    return out


def _filter_journey_instances_from_silver(
    db: Session,
    *,
    definition: JourneyDefinition,
    date_from: date,
    date_to: date,
    channel_group: Optional[str],
    campaign_id: Optional[str],
    device: Optional[str],
    country: Optional[str],
    path_hash: Optional[str],
) -> List[Tuple[Dict[str, Any], Dict[str, Optional[str]], str, float]]:
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    journeys = load_silver_journeys(
        db,
        start_dt=start_dt,
        end_dt=end_dt,
        conversion_key=definition.conversion_kpi_id,
    )
    if not journeys:
        return []

    out: List[Tuple[Dict[str, Any], Dict[str, Optional[str]], str, float]] = []
    for journey in journeys:
        payload = journey["payload"]
        conversion_ts = journey["conversion_ts"]
        steps, _, dims = _build_journey_steps(
            payload,
            conversion_ts=conversion_ts,
            lookback_window_days=definition.lookback_window_days,
        )
        ph = _path_hash(steps) if steps else ""
        if path_hash and ph != path_hash:
            continue
        if channel_group and (dims.get("channel_group") or "").lower() != channel_group.lower():
            continue
        if campaign_id and (dims.get("campaign_id") or "") != campaign_id:
            continue
        if device and (dims.get("device") or "").lower() != device.lower():
            continue
        if country and (dims.get("country") or "").lower() != country.lower():
            continue
        out.append((payload, dims, ph, float(journey["gross_revenue_total"] or 0.0)))
    return out


_COMPRESSIBLE_ATTRIBUTION_MODELS = {"last_touch", "first_touch", "linear", "position_based"}


def _can_compress_attribution(model: str, *, include_campaign: bool) -> bool:
    return model in _COMPRESSIBLE_ATTRIBUTION_MODELS and not include_campaign


def _compress_legacy_journeys(journeys: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[Tuple[str, ...], str], Dict[str, Any]] = {}

    for journey in journeys:
        touchpoints = journey.get("touchpoints") or []
        if not isinstance(touchpoints, list) or not touchpoints:
            continue
        channel_signature = tuple(str((tp or {}).get("channel") or "unknown") for tp in touchpoints if isinstance(tp, dict))
        if not channel_signature:
            continue
        interaction_type = str(
            (((journey.get("meta") or {}).get("interaction_summary") or {}).get("path_type"))
            or journey.get("interaction_path_type")
            or "unknown"
        )
        key = (channel_signature, interaction_type)
        bucket = grouped.get(key)
        if bucket is None:
            sample_touchpoints: List[Dict[str, Any]] = []
            for idx, channel in enumerate(channel_signature):
                sample_touchpoints.append(
                    {
                        "channel": channel,
                        "timestamp": f"2026-01-01T00:{idx:02d}:00Z",
                    }
                )
            bucket = {
                "touchpoints": sample_touchpoints,
                "converted": True,
                "conversion_value": 0.0,
                "_revenue_entries": [
                    {
                        "value_in_base": 0.0,
                        "net_value_in_base": 0.0,
                        "gross_value": 0.0,
                        "net_value": 0.0,
                        "refunded_value": 0.0,
                        "cancelled_value": 0.0,
                        "gross_conversions": 0.0,
                        "net_conversions": 0.0,
                        "invalid_leads": 0.0,
                        "valid_leads": 0.0,
                    }
                ],
                "meta": {"interaction_summary": {"path_type": interaction_type}},
                "interaction_path_type": interaction_type,
            }
            grouped[key] = bucket

        summary = journey.get("_revenue_entries")
        if not isinstance(summary, list) or not summary:
            gross_value = float(journey.get("conversion_value") or 0.0)
            net_value = gross_value
            gross_conversions = 1.0 if journey.get("converted", True) else 0.0
            net_conversions = gross_conversions
            refunded_value = 0.0
            cancelled_value = 0.0
            invalid_leads = 0.0
            valid_leads = gross_conversions
        else:
            gross_value = 0.0
            net_value = 0.0
            gross_conversions = 0.0
            net_conversions = 0.0
            refunded_value = 0.0
            cancelled_value = 0.0
            invalid_leads = 0.0
            valid_leads = 0.0
            for entry in summary:
                if not isinstance(entry, dict):
                    continue
                gross_value += float(entry.get("gross_value", entry.get("value_in_base", 0.0)) or 0.0)
                net_value += float(entry.get("net_value", entry.get("net_value_in_base", entry.get("value_in_base", 0.0))) or 0.0)
                gross_conversions += float(entry.get("gross_conversions", 0.0) or 0.0)
                net_conversions += float(entry.get("net_conversions", 0.0) or 0.0)
                refunded_value += float(entry.get("refunded_value", 0.0) or 0.0)
                cancelled_value += float(entry.get("cancelled_value", 0.0) or 0.0)
                invalid_leads += float(entry.get("invalid_leads", 0.0) or 0.0)
                valid_leads += float(entry.get("valid_leads", 0.0) or 0.0)

        bucket["conversion_value"] += gross_value
        revenue_entry = bucket["_revenue_entries"][0]
        revenue_entry["value_in_base"] += gross_value
        revenue_entry["gross_value"] += gross_value
        revenue_entry["net_value_in_base"] += net_value
        revenue_entry["net_value"] += net_value
        revenue_entry["gross_conversions"] += gross_conversions
        revenue_entry["net_conversions"] += net_conversions
        revenue_entry["refunded_value"] += refunded_value
        revenue_entry["cancelled_value"] += cancelled_value
        revenue_entry["invalid_leads"] += invalid_leads
        revenue_entry["valid_leads"] += valid_leads

    return list(grouped.values())


def build_journey_attribution_summary(
    db: Session,
    *,
    definition: JourneyDefinition,
    date_from: str,
    date_to: str,
    model: str = "linear",
    mode: str = "conversion_only",
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
    path_hash: Optional[str] = None,
    include_campaign: bool = False,
    settings_obj: Any = None,
) -> Dict[str, Any]:
    if model not in ATTRIBUTION_MODELS:
        raise ValueError(f"Unknown model '{model}'. Available: {', '.join(ATTRIBUTION_MODELS)}")

    d_from = _parse_date(date_from)
    d_to = _parse_date(date_to)
    if d_from > d_to:
        d_from, d_to = d_to, d_from

    silver_instances = _filter_journey_instances_from_silver(
        db,
        definition=definition,
        date_from=d_from,
        date_to=d_to,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
        path_hash=path_hash,
    )

    journeys: List[Dict[str, Any]] = []
    observed_total = 0.0
    if silver_instances:
        for payload, _dims, _ph, gross_value in silver_instances:
            journeys.append(v2_to_legacy(payload))
            observed_total += float(gross_value or 0.0)
    else:
        filtered_rows = _filter_conversion_paths(
            db,
            definition=definition,
            date_from=d_from,
            date_to=d_to,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            path_hash=path_hash,
        )
        for row, payload, _dims, _ph in filtered_rows:
            journeys.append(v2_to_legacy(payload))
            fallback_value = payload.get("conversion_value")
            if fallback_value in (None, "", []):
                conversions = payload.get("conversions")
                first_conversion = conversions[0] if isinstance(conversions, list) and conversions and isinstance(conversions[0], dict) else {}
                fallback_value = first_conversion.get("value") if isinstance(first_conversion, dict) else None
            try:
                observed_total += float(fallback_value or 0.0)
            except Exception:
                continue

    used_compressed_journeys = False
    attribution_journeys = journeys
    if journeys and _can_compress_attribution(model, include_campaign=include_campaign):
        compressed = _compress_legacy_journeys(journeys)
        if compressed and len(compressed) < len(journeys):
            attribution_journeys = compressed
            used_compressed_journeys = True

    kwargs = _model_kwargs(model, settings_obj) if settings_obj is not None else {}
    attribution = run_attribution(attribution_journeys, model=model, **kwargs) if attribution_journeys else {
        "model": model,
        "channels": [],
        "total_value": 0.0,
        "total_conversions": 0,
    }

    by_group: Dict[str, float] = {}
    by_channel = []
    for ch in attribution.get("channels", []):
        channel_name = str(ch.get("channel") or "unknown")
        attributed_value = float(ch.get("attributed_value") or 0.0)
        attributed_share = float(ch.get("attributed_share") or 0.0)
        by_channel.append(
            {
                "channel": channel_name,
                "attributed_value": round(attributed_value, 2),
                "attributed_share": round(attributed_share, 4),
            }
        )
        group = _channel_group(str(ch.get("channel") or "unknown"))
        by_group[group] = by_group.get(group, 0.0) + attributed_value

    by_channel_group = []
    total_attr = float(sum(by_group.values()))
    for group, value in sorted(by_group.items(), key=lambda kv: -kv[1]):
        by_channel_group.append(
            {
                "channel_group": group,
                "attributed_value": round(value, 2),
                "attributed_share": round((value / total_attr), 4) if total_attr > 0 else 0.0,
            }
        )

    by_campaign = []
    if include_campaign:
        camp_res = run_attribution_campaign(journeys, model=model, **kwargs) if journeys else {"channels": []}
        for row in camp_res.get("channels", []):
            step = str(row.get("channel") or "")
            channel = step.split(":", 1)[0] if ":" in step else step
            campaign = step.split(":", 1)[1] if ":" in step else None
            by_campaign.append(
                {
                    "campaign_key": step,
                    "channel": channel,
                    "campaign": campaign,
                    "attributed_value": float(row.get("attributed_value") or 0.0),
                    "attributed_share": float(row.get("attributed_share") or 0.0),
                }
            )

    delta_abs = round(total_attr - observed_total, 4)
    delta_pct = round((delta_abs / observed_total), 6) if observed_total > 0 else 0.0

    return {
        "journey_definition_id": definition.id,
        "model": model,
        "mode": mode,
        "date_from": d_from.isoformat(),
        "date_to": d_to.isoformat(),
        "path_hash": path_hash,
        "approximation": {
            "method": "compressed_journey_instances" if used_compressed_journeys else "sampled_journey_instances",
            "note": (
                "Credit is computed on compressed repeated journey instances for deterministic path-based models."
                if used_compressed_journeys
                else "Credit is computed by running the selected attribution model on filtered journey instances from conversion_paths."
            ),
            "tolerance_pct": 0.05,
            "compressed_journeys": len(attribution_journeys) if used_compressed_journeys else None,
        },
        "totals": {
            "journeys": len(journeys),
            "total_value_observed": round(observed_total, 2),
            "total_value_attributed": round(total_attr, 2),
            "delta_abs": delta_abs,
            "delta_pct": delta_pct,
        },
        "by_channel_group": by_channel_group,
        "by_channel": by_channel,
        "by_campaign": by_campaign,
    }
