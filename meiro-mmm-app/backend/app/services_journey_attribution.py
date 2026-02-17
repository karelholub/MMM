"""Journey attribution summary adapter for paths/flow UIs.

Uses existing attribution models from app.attribution_engine and samples journey
instances from conversion_paths with journey-definition filters.
"""

from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy.orm import Session

from .attribution_engine import ATTRIBUTION_MODELS, run_attribution, run_attribution_campaign
from .models_config_dq import ConversionPath, JourneyDefinition
from .services_conversions import v2_to_legacy
from .services_journey_aggregates import _build_journey_steps, _path_hash

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
) -> List[Tuple[ConversionPath, Dict[str, Any], Dict[str, Optional[str]], str]]:
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    q = db.query(ConversionPath).filter(ConversionPath.conversion_ts >= start_dt, ConversionPath.conversion_ts < end_dt)
    if definition.conversion_kpi_id:
        q = q.filter(ConversionPath.conversion_key == definition.conversion_kpi_id)
    rows = q.all()

    out: List[Tuple[ConversionPath, Dict[str, Any], Dict[str, Optional[str]], str]] = []
    for row in rows:
        payload = row.path_json if isinstance(row.path_json, dict) else {}
        conv_ts = row.conversion_ts
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
        out.append((row, payload, dims, ph))
    return out


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

    journeys: List[Dict[str, Any]] = []
    for _row, payload, _dims, _ph in filtered_rows:
        journeys.append(v2_to_legacy(payload))

    kwargs = _model_kwargs(model, settings_obj) if settings_obj is not None else {}
    attribution = run_attribution(journeys, model=model, **kwargs) if journeys else {
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

    observed_total = float(sum((j.get("conversion_value") or 0.0) for j in journeys))
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
            "method": "sampled_journey_instances",
            "note": "Credit is computed by running the selected attribution model on filtered journey instances from conversion_paths.",
            "tolerance_pct": 0.05,
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
