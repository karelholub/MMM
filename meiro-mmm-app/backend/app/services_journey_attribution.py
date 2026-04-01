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
from .models_config_dq import ConversionPath, JourneyDefinition, JourneyInstanceFact, JourneyRoleFact, JourneyStepFact
from .services_canonical_facts import load_preferred_journey_rows
from .services_conversions import conversion_path_payload, v2_to_legacy
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
    source, journeys = load_preferred_journey_rows(
        db,
        start_dt=start_dt,
        end_dt=end_dt,
        conversion_key=definition.conversion_kpi_id,
    )
    if source == "instance":
        out: List[Tuple[Dict[str, Any], Dict[str, Optional[str]], str, float]] = []
        for journey in journeys:
            conversion_ts = journey["conversion_ts"]
            if not isinstance(conversion_ts, datetime):
                continue
            dims = {
                "channel_group": journey.get("channel_group"),
                "last_touch_channel": journey.get("last_touch_channel"),
                "campaign_id": journey.get("campaign_id"),
                "device": journey.get("device"),
                "country": journey.get("country"),
            }
            ph = str(journey.get("path_hash") or "")
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
            touchpoints = []
            for step in journey.get("steps") or []:
                step_name = str(step.get("step_name") or "")
                step_ts = step.get("step_ts")
                if step_name == "Purchase / Lead Won (conversion)" or not isinstance(step_ts, datetime):
                    continue
                touchpoints.append(
                    {
                        "ts": step_ts.isoformat(),
                        "timestamp": step_ts.isoformat(),
                        "channel": step.get("channel"),
                        "campaign": step.get("campaign"),
                        "event_name": step.get("event_name") or step_name,
                    }
                )
            payload = {
                "journey_id": journey["conversion_id"],
                "customer": {"id": journey.get("profile_id") or journey["conversion_id"]},
                "kpi_type": journey.get("conversion_key"),
                "touchpoints": touchpoints,
                "conversions": [
                    {
                        "id": journey["conversion_id"],
                        "name": journey.get("conversion_key"),
                        "ts": conversion_ts.isoformat(),
                        "value": float(journey.get("gross_revenue_total") or 0.0),
                    }
                ],
                "device": journey.get("device"),
                "country": journey.get("country"),
                "interaction_path_type": journey.get("interaction_path_type"),
                "meta": {"interaction_summary": {"path_type": journey.get("interaction_path_type")}},
                "_revenue_entries": [
                    {
                        "value_in_base": float(journey.get("gross_revenue_total") or 0.0),
                        "net_value_in_base": float(journey.get("net_revenue_total") or 0.0),
                        "gross_value": float(journey.get("gross_revenue_total") or 0.0),
                        "net_value": float(journey.get("net_revenue_total") or 0.0),
                        "refunded_value": 0.0,
                        "cancelled_value": 0.0,
                        "gross_conversions": float(journey.get("gross_conversions_total") or 0.0),
                        "net_conversions": float(journey.get("net_conversions_total") or 0.0),
                        "invalid_leads": 0.0,
                        "valid_leads": float(journey.get("net_conversions_total") or 0.0),
                    }
                ],
            }
            out.append((payload, dims, ph, float(journey["gross_revenue_total"] or 0.0)))
        if out:
            return out
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
_ROLE_FACT_MODELS = {"first_touch", "last_touch"}


def _can_compress_attribution(model: str, *, include_campaign: bool) -> bool:
    return model in _COMPRESSIBLE_ATTRIBUTION_MODELS and not include_campaign


def _role_key_for_model(model: str) -> Optional[str]:
    if model == "first_touch":
        return "first_touch"
    if model == "last_touch":
        return "last_touch"
    return None


def _position_based_weights(step_count: int, *, first_pct: float, last_pct: float) -> List[float]:
    if step_count <= 0:
        return []
    if step_count == 1:
        return [1.0]
    if step_count == 2:
        total = first_pct + last_pct
        if total <= 0:
            return [0.5, 0.5]
        return [first_pct / total, last_pct / total]
    middle_pct = 1.0 - first_pct - last_pct
    middle_share = middle_pct / (step_count - 2) if step_count > 2 else 0.0
    return [first_pct] + ([middle_share] * (step_count - 2)) + [last_pct]


def _build_step_fact_summary(
    db: Session,
    *,
    definition: JourneyDefinition,
    d_from: date,
    d_to: date,
    model: str,
    channel_group: Optional[str],
    campaign_id: Optional[str],
    device: Optional[str],
    country: Optional[str],
    path_hash: Optional[str],
    include_campaign: bool,
    mode: str,
    settings_obj: Any,
) -> Optional[Dict[str, Any]]:
    if model not in {"linear", "position_based", "time_decay", "markov"}:
        return None
    start_dt = datetime.combine(d_from, dt_time.min)
    end_dt = datetime.combine(d_to + timedelta(days=1), dt_time.min)
    q = db.query(
        JourneyInstanceFact.conversion_id,
        JourneyInstanceFact.path_hash,
        JourneyInstanceFact.channel_group,
        JourneyInstanceFact.campaign_id,
        JourneyInstanceFact.device,
        JourneyInstanceFact.country,
        JourneyInstanceFact.gross_revenue_total,
        JourneyInstanceFact.net_revenue_total,
        JourneyInstanceFact.gross_conversions_total,
        JourneyInstanceFact.net_conversions_total,
    ).filter(
        JourneyInstanceFact.conversion_ts >= start_dt,
        JourneyInstanceFact.conversion_ts < end_dt,
    )
    if definition.conversion_kpi_id:
        q = q.filter(JourneyInstanceFact.conversion_key == definition.conversion_kpi_id)
    if path_hash:
        q = q.filter(JourneyInstanceFact.path_hash == path_hash)
    if channel_group:
        q = q.filter(JourneyInstanceFact.channel_group == channel_group)
    if campaign_id:
        q = q.filter(JourneyInstanceFact.campaign_id == campaign_id)
    if device:
        q = q.filter(JourneyInstanceFact.device == device)
    if country:
        q = q.filter(JourneyInstanceFact.country == country)
    instance_rows = q.all()
    if not instance_rows:
        return None

    instance_by_conversion: Dict[str, Any] = {
        str(row[0] or ""): row
        for row in instance_rows
        if str(row[0] or "")
    }
    conversion_ids = list(instance_by_conversion.keys())
    if not conversion_ids:
        return None

    step_rows = (
        db.query(
            JourneyStepFact.conversion_id,
            JourneyStepFact.ordinal,
            JourneyStepFact.step_name,
            JourneyStepFact.step_ts,
            JourneyStepFact.channel,
            JourneyStepFact.campaign,
        )
        .filter(JourneyStepFact.conversion_id.in_(conversion_ids))
        .order_by(JourneyStepFact.conversion_id.asc(), JourneyStepFact.ordinal.asc())
        .all()
    )
    steps_by_conversion: Dict[str, List[Any]] = {}
    for row in step_rows:
        conversion_id = str(row[0] or "")
        if not conversion_id:
            continue
        if str(row[2] or "") == "Purchase / Lead Won (conversion)":
            continue
        steps_by_conversion.setdefault(conversion_id, []).append(row)

    kwargs = _model_kwargs(model, settings_obj) if settings_obj is not None else {}
    by_channel_totals: Dict[str, float] = {}
    by_group_totals: Dict[str, float] = {}
    by_campaign_totals: Dict[Tuple[str, Optional[str]], float] = {}
    observed_total = 0.0
    journey_count = 0
    step_fact_journeys: List[Dict[str, Any]] = []

    for conversion_id, instance in instance_by_conversion.items():
        steps = steps_by_conversion.get(conversion_id) or []
        if not steps:
            continue
        gross_revenue = float(instance[6] or 0.0)
        observed_total += gross_revenue
        journey_count += 1
        if model in {"linear", "position_based"}:
            first_pct = float(kwargs.get("first_pct", 0.4) or 0.4)
            last_pct = float(kwargs.get("last_pct", 0.4) or 0.4)
            if model == "linear":
                weights = [1.0 / len(steps)] * len(steps)
            else:
                weights = _position_based_weights(len(steps), first_pct=first_pct, last_pct=last_pct)
            for step_row, weight in zip(steps, weights):
                channel_name = str(step_row[4] or "unknown")
                attributed_value = gross_revenue * weight
                by_channel_totals[channel_name] = by_channel_totals.get(channel_name, 0.0) + attributed_value
                group = _channel_group(channel_name)
                by_group_totals[group] = by_group_totals.get(group, 0.0) + attributed_value
                if include_campaign:
                    campaign_name = str(step_row[5] or "").strip() or None
                    key = (channel_name, campaign_name)
                    by_campaign_totals[key] = by_campaign_totals.get(key, 0.0) + attributed_value
        else:
            touchpoints = []
            for step_row in steps:
                channel_name = str(step_row[4] or "unknown")
                campaign_name = str(step_row[5] or "").strip() or None
                step_ts = step_row[3]
                timestamp = step_ts.isoformat() if isinstance(step_ts, datetime) else f"2026-01-01T00:{int(step_row[1] or 0):02d}:00Z"
                touchpoints.append(
                    {
                        "channel": channel_name,
                        "campaign": campaign_name,
                        "timestamp": timestamp,
                    }
                )
            step_fact_journeys.append(
                {
                    "customer_id": conversion_id,
                    "touchpoints": touchpoints,
                    "converted": True,
                    "conversion_value": gross_revenue,
                    "interaction_path_type": "unknown",
                    "_revenue_entries": [
                        {
                            "value_in_base": gross_revenue,
                            "net_value_in_base": float(instance[7] or 0.0),
                            "gross_value": gross_revenue,
                            "net_value": float(instance[7] or 0.0),
                            "refunded_value": 0.0,
                            "cancelled_value": 0.0,
                            "gross_conversions": float(instance[8] or 0.0),
                            "net_conversions": float(instance[9] or 0.0),
                            "invalid_leads": 0.0,
                            "valid_leads": float(instance[9] or 0.0),
                        }
                    ],
                }
            )

    if model in {"time_decay", "markov"}:
        if not step_fact_journeys:
            return None
        attribution = run_attribution(step_fact_journeys, model=model, **kwargs) if kwargs else run_attribution(step_fact_journeys, model=model)
        total_attr = float(sum(float(row.get("attributed_value") or 0.0) for row in attribution.get("channels", [])))
        for row in attribution.get("channels", []):
            channel_name = str(row.get("channel") or "unknown")
            value = float(row.get("attributed_value") or 0.0)
            by_channel_totals[channel_name] = value
            group = _channel_group(channel_name)
            by_group_totals[group] = by_group_totals.get(group, 0.0) + value
        if include_campaign:
            campaign_result = run_attribution_campaign(step_fact_journeys, model=model, **kwargs) if kwargs else run_attribution_campaign(step_fact_journeys, model=model)
            for row in campaign_result.get("channels", []):
                step_name = str(row.get("channel") or "")
                channel_name = step_name.split(":", 1)[0] if ":" in step_name else step_name
                campaign_name = step_name.split(":", 1)[1] if ":" in step_name else None
                by_campaign_totals[(channel_name, campaign_name)] = float(row.get("attributed_value") or 0.0)
    else:
        total_attr = float(sum(by_channel_totals.values()))

    if journey_count <= 0:
        return None
    by_channel = [
        {
            "channel": channel,
            "attributed_value": round(value, 2),
            "attributed_share": round((value / total_attr), 4) if total_attr > 0 else 0.0,
        }
        for channel, value in sorted(by_channel_totals.items(), key=lambda item: -item[1])
    ]
    by_channel_group = [
        {
            "channel_group": group,
            "attributed_value": round(value, 2),
            "attributed_share": round((value / total_attr), 4) if total_attr > 0 else 0.0,
        }
        for group, value in sorted(by_group_totals.items(), key=lambda item: -item[1])
    ]
    by_campaign = []
    if include_campaign:
        for (channel_name, campaign_name), value in sorted(by_campaign_totals.items(), key=lambda item: -item[1]):
            campaign_key = f"{channel_name}:{campaign_name}" if campaign_name else channel_name
            by_campaign.append(
                {
                    "campaign_key": campaign_key,
                    "channel": channel_name,
                    "campaign": campaign_name,
                    "attributed_value": float(value),
                    "attributed_share": float(value / total_attr) if total_attr > 0 else 0.0,
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
            "method": "step_facts_exact",
            "note": "Credit is computed directly from persisted journey step facts for deterministic multi-touch models.",
            "tolerance_pct": 0.0,
            "compressed_journeys": None,
        },
        "totals": {
            "journeys": journey_count,
            "total_value_observed": round(observed_total, 2),
            "total_value_attributed": round(total_attr, 2),
            "delta_abs": delta_abs,
            "delta_pct": delta_pct,
        },
        "by_channel_group": by_channel_group,
        "by_channel": by_channel,
        "by_campaign": by_campaign,
    }


def _build_role_fact_summary(
    db: Session,
    *,
    definition: JourneyDefinition,
    d_from: date,
    d_to: date,
    model: str,
    channel_group: Optional[str],
    campaign_id: Optional[str],
    device: Optional[str],
    country: Optional[str],
    path_hash: Optional[str],
    include_campaign: bool,
    mode: str,
) -> Optional[Dict[str, Any]]:
    role_key = _role_key_for_model(model)
    if role_key is None:
        return None
    start_dt = datetime.combine(d_from, dt_time.min)
    end_dt = datetime.combine(d_to + timedelta(days=1), dt_time.min)
    q = db.query(JourneyRoleFact).filter(
        JourneyRoleFact.role_key == role_key,
        JourneyRoleFact.conversion_ts >= start_dt,
        JourneyRoleFact.conversion_ts < end_dt,
    )
    if definition.conversion_kpi_id:
        q = q.filter(JourneyRoleFact.conversion_key == definition.conversion_kpi_id)
    if path_hash:
        q = q.filter(JourneyRoleFact.path_hash == path_hash)
    if channel_group:
        q = q.filter(JourneyRoleFact.channel_group == channel_group)
    if campaign_id:
        q = q.filter(JourneyRoleFact.campaign == campaign_id)
    if device:
        q = q.filter(JourneyRoleFact.device == device)
    if country:
        q = q.filter(JourneyRoleFact.country == country)
    rows = q.all()
    if not rows:
        return None

    by_channel_totals: Dict[str, float] = {}
    by_group_totals: Dict[str, float] = {}
    by_campaign_totals: Dict[Tuple[str, Optional[str]], float] = {}
    observed_total = 0.0
    seen_conversions = set()

    for row in rows:
        channel_name = str(row.channel or "unknown")
        gross_revenue = float(row.gross_revenue_total or 0.0)
        by_channel_totals[channel_name] = by_channel_totals.get(channel_name, 0.0) + gross_revenue
        group = _channel_group(channel_name)
        by_group_totals[group] = by_group_totals.get(group, 0.0) + gross_revenue
        if include_campaign:
            camp = str(row.campaign or "").strip() or None
            by_campaign_totals[(channel_name, camp)] = by_campaign_totals.get((channel_name, camp), 0.0) + gross_revenue
        conversion_id = str(row.conversion_id or "")
        if conversion_id and conversion_id not in seen_conversions:
            seen_conversions.add(conversion_id)
            observed_total += gross_revenue

    total_attr = float(sum(by_channel_totals.values()))
    by_channel = [
        {
            "channel": channel,
            "attributed_value": round(value, 2),
            "attributed_share": round((value / total_attr), 4) if total_attr > 0 else 0.0,
        }
        for channel, value in sorted(by_channel_totals.items(), key=lambda item: -item[1])
    ]
    by_channel_group = [
        {
            "channel_group": group,
            "attributed_value": round(value, 2),
            "attributed_share": round((value / total_attr), 4) if total_attr > 0 else 0.0,
        }
        for group, value in sorted(by_group_totals.items(), key=lambda item: -item[1])
    ]
    by_campaign = []
    if include_campaign:
        for (channel_name, campaign_name), value in sorted(by_campaign_totals.items(), key=lambda item: -item[1]):
            campaign_key = f"{channel_name}:{campaign_name}" if campaign_name else channel_name
            by_campaign.append(
                {
                    "campaign_key": campaign_key,
                    "channel": channel_name,
                    "campaign": campaign_name,
                    "attributed_value": float(value),
                    "attributed_share": float(value / total_attr) if total_attr > 0 else 0.0,
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
            "method": "role_facts_exact",
            "note": "Credit is computed directly from persisted journey role facts for deterministic touch models.",
            "tolerance_pct": 0.0,
            "compressed_journeys": None,
        },
        "totals": {
            "journeys": len(seen_conversions),
            "total_value_observed": round(observed_total, 2),
            "total_value_attributed": round(total_attr, 2),
            "delta_abs": delta_abs,
            "delta_pct": delta_pct,
        },
        "by_channel_group": by_channel_group,
        "by_channel": by_channel,
        "by_campaign": by_campaign,
    }


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

    role_fact_summary = _build_role_fact_summary(
        db,
        definition=definition,
        d_from=d_from,
        d_to=d_to,
        model=model,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
        path_hash=path_hash,
        include_campaign=include_campaign,
        mode=mode,
    )
    if role_fact_summary is not None:
        return role_fact_summary

    step_fact_summary = _build_step_fact_summary(
        db,
        definition=definition,
        d_from=d_from,
        d_to=d_to,
        model=model,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
        path_hash=path_hash,
        include_campaign=include_campaign,
        mode=mode,
        settings_obj=settings_obj,
    )
    if step_fact_summary is not None:
        return step_fact_summary

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
