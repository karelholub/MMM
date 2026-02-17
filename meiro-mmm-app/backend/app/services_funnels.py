"""Funnel definition CRUD + result computation (aggregates-first, raw fallback)."""

from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
import math
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import ConversionPath, FunnelDefinition, JourneyDefinition, JourneyTransitionDaily
from .services_journey_aggregates import STEP_ORGANIC_LANDING, STEP_PAID_LANDING


def _percentile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    q = max(0.0, min(1.0, q))
    idx = (len(ordered) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _serialize_funnel(item: FunnelDefinition) -> Dict[str, Any]:
    return {
        "id": item.id,
        "journey_definition_id": item.journey_definition_id,
        "workspace_id": item.workspace_id,
        "user_id": item.user_id,
        "name": item.name,
        "description": item.description,
        "steps": item.steps_json or [],
        "counting_method": item.counting_method,
        "window_days": item.window_days,
        "is_archived": bool(item.is_archived),
        "created_by": item.created_by,
        "updated_by": item.updated_by,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    }


def list_funnels(
    db: Session,
    *,
    workspace_id: str = "default",
    user_id: Optional[str] = None,
    journey_definition_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    q = db.query(FunnelDefinition).filter(FunnelDefinition.workspace_id == workspace_id, FunnelDefinition.is_archived == False)  # noqa: E712
    if user_id:
        q = q.filter(FunnelDefinition.user_id == user_id)
    if journey_definition_id:
        q = q.filter(FunnelDefinition.journey_definition_id == journey_definition_id)
    rows = q.order_by(FunnelDefinition.updated_at.desc()).all()
    return [_serialize_funnel(r) for r in rows]


def create_funnel(
    db: Session,
    *,
    journey_definition_id: str,
    workspace_id: str,
    user_id: str,
    name: str,
    description: Optional[str],
    steps: Sequence[str],
    counting_method: str,
    window_days: int,
    actor: str,
) -> Dict[str, Any]:
    now = datetime.utcnow()
    item = FunnelDefinition(
        id=str(uuid.uuid4()),
        journey_definition_id=journey_definition_id,
        workspace_id=(workspace_id or "default").strip() or "default",
        user_id=(user_id or "default").strip() or "default",
        name=name.strip(),
        description=description,
        steps_json=[str(s).strip() for s in steps if str(s).strip()],
        counting_method=counting_method,
        window_days=max(1, min(int(window_days), 365)),
        is_archived=False,
        created_by=actor,
        updated_by=actor,
        created_at=now,
        updated_at=now,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return _serialize_funnel(item)


def get_funnel(db: Session, funnel_id: str) -> Optional[FunnelDefinition]:
    return db.get(FunnelDefinition, funnel_id)


def _from_step_label(channel: str) -> str:
    ch = (channel or "").strip().lower()
    if ch in {"paid", "paid_search", "paid_social", "display", "affiliate"}:
        return STEP_PAID_LANDING
    return STEP_ORGANIC_LANDING


def _extract_steps_with_ts(path_payload: Dict[str, Any], conversion_ts: datetime) -> List[Tuple[str, datetime]]:
    tps = path_payload.get("touchpoints") or []
    if not isinstance(tps, list):
        tps = []
    rows: List[Tuple[datetime, Dict[str, Any]]] = []
    for tp in tps:
        if not isinstance(tp, dict):
            continue
        ts_raw = tp.get("timestamp") or tp.get("ts") or tp.get("event_ts")
        try:
            ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        except Exception:
            continue
        rows.append((ts, tp))
    rows.sort(key=lambda it: it[0])
    out: List[Tuple[str, datetime]] = []
    prev = None
    for idx, (ts, tp) in enumerate(rows):
        if idx == 0:
            step = _from_step_label(str(tp.get("channel") or ""))
        else:
            ev = str(tp.get("event") or tp.get("event_name") or tp.get("name") or "").lower()
            if any(tok in ev for tok in ("add_to_cart", "form_start")):
                step = "Add to Cart / Form Start"
            elif any(tok in ev for tok in ("checkout", "form_submit", "purchase")):
                step = "Checkout / Form Submit"
            else:
                step = "Product View / Content View"
        if step == prev:
            continue
        out.append((step, ts))
        prev = step
    out.append(("Purchase / Lead Won (conversion)", conversion_ts))
    return out


def _compute_results_from_transitions(
    db: Session,
    *,
    journey_definition_id: str,
    steps: Sequence[str],
    date_from: date,
    date_to: date,
    device: Optional[str],
    channel_group: Optional[str],
    country: Optional[str],
    campaign_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    if len(steps) < 2:
        return None
    pair_counts: List[int] = []
    for src, tgt in zip(steps, steps[1:]):
        q = db.query(func.sum(JourneyTransitionDaily.count_profiles)).filter(
            JourneyTransitionDaily.journey_definition_id == journey_definition_id,
            JourneyTransitionDaily.date >= date_from,
            JourneyTransitionDaily.date <= date_to,
            JourneyTransitionDaily.from_step == src,
            JourneyTransitionDaily.to_step == tgt,
        )
        if device:
            q = q.filter(JourneyTransitionDaily.device == device)
        if channel_group:
            q = q.filter(JourneyTransitionDaily.channel_group == channel_group)
        if country:
            q = q.filter(JourneyTransitionDaily.country == country)
        if campaign_id:
            q = q.filter(JourneyTransitionDaily.campaign_id == campaign_id)
        c = int(q.scalar() or 0)
        pair_counts.append(c)
    if not pair_counts or max(pair_counts) <= 0:
        return None

    step_counts: List[int] = [pair_counts[0]]
    for idx in range(1, len(steps)):
        prev = step_counts[idx - 1]
        pair_val = pair_counts[idx - 1] if idx - 1 < len(pair_counts) else prev
        step_counts.append(min(prev, pair_val))

    # Top 5 breakdowns from first pair.
    first_src, first_tgt = steps[0], steps[1]
    qd = db.query(JourneyTransitionDaily.device, func.sum(JourneyTransitionDaily.count_profiles)).filter(
        JourneyTransitionDaily.journey_definition_id == journey_definition_id,
        JourneyTransitionDaily.date >= date_from,
        JourneyTransitionDaily.date <= date_to,
        JourneyTransitionDaily.from_step == first_src,
        JourneyTransitionDaily.to_step == first_tgt,
    ).group_by(JourneyTransitionDaily.device)
    qc = db.query(JourneyTransitionDaily.channel_group, func.sum(JourneyTransitionDaily.count_profiles)).filter(
        JourneyTransitionDaily.journey_definition_id == journey_definition_id,
        JourneyTransitionDaily.date >= date_from,
        JourneyTransitionDaily.date <= date_to,
        JourneyTransitionDaily.from_step == first_src,
        JourneyTransitionDaily.to_step == first_tgt,
    ).group_by(JourneyTransitionDaily.channel_group)
    if country:
        qd = qd.filter(JourneyTransitionDaily.country == country)
        qc = qc.filter(JourneyTransitionDaily.country == country)
    if campaign_id:
        qd = qd.filter(JourneyTransitionDaily.campaign_id == campaign_id)
        qc = qc.filter(JourneyTransitionDaily.campaign_id == campaign_id)
    if device:
        qc = qc.filter(JourneyTransitionDaily.device == device)
    if channel_group:
        qd = qd.filter(JourneyTransitionDaily.channel_group == channel_group)
    device_breakdown = [{"key": str(k or "unknown"), "count": int(v or 0)} for k, v in qd.all() if int(v or 0) > 0][:5]
    channel_breakdown = [{"key": str(k or "unknown"), "count": int(v or 0)} for k, v in qc.all() if int(v or 0) > 0][:5]

    return {
        "step_counts": step_counts,
        "time_between": [],  # transitions aggregates do not store timings
        "breakdown_device": device_breakdown,
        "breakdown_channel_group": channel_breakdown,
        "source": "aggregates",
    }


def _compute_results_from_raw(
    db: Session,
    *,
    journey_definition: JourneyDefinition,
    steps: Sequence[str],
    date_from: date,
    date_to: date,
    device: Optional[str],
    channel_group: Optional[str],
    country: Optional[str],
    campaign_id: Optional[str],
) -> Dict[str, Any]:
    if not steps:
        return {"step_counts": [], "time_between": [], "breakdown_device": [], "breakdown_channel_group": [], "source": "raw"}
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    q = db.query(ConversionPath).filter(ConversionPath.conversion_ts >= start_dt, ConversionPath.conversion_ts < end_dt)
    if journey_definition.conversion_kpi_id:
        q = q.filter(ConversionPath.conversion_key == journey_definition.conversion_kpi_id)
    rows = q.all()

    step_counts = [0 for _ in steps]
    pair_times: Dict[Tuple[str, str], List[float]] = {(a, b): [] for a, b in zip(steps, steps[1:])}
    by_device: Dict[str, int] = {}
    by_channel: Dict[str, int] = {}

    for row in rows:
        payload = row.path_json if isinstance(row.path_json, dict) else {}
        payload_device = str(payload.get("device") or "").strip()
        payload_country = str(payload.get("country") or "").strip().upper()
        if device and payload_device and payload_device != device:
            continue
        if country and payload_country and payload_country != country.upper():
            continue
        seq = _extract_steps_with_ts(payload, row.conversion_ts)
        if not seq:
            continue
        mapped_steps = [s for s, _ in seq]
        if channel_group:
            first = mapped_steps[0] if mapped_steps else ""
            if channel_group == "paid" and first != STEP_PAID_LANDING:
                continue
            if channel_group == "organic" and first != STEP_ORGANIC_LANDING:
                continue
        if campaign_id:
            tps = payload.get("touchpoints") or []
            last_campaign = None
            if isinstance(tps, list) and tps:
                cand = tps[-1].get("campaign") if isinstance(tps[-1], dict) else None
                if isinstance(cand, dict):
                    last_campaign = cand.get("id") or cand.get("name")
                else:
                    last_campaign = cand
            if str(last_campaign or "") != campaign_id:
                continue

        matched_idx = -1
        matched_positions: List[int] = []
        for target in steps:
            found = None
            for pos in range(matched_idx + 1, len(mapped_steps)):
                if mapped_steps[pos] == target:
                    found = pos
                    break
            if found is None:
                break
            matched_idx = found
            matched_positions.append(found)
            step_counts[len(matched_positions) - 1] += 1
        if matched_positions:
            by_device[payload_device or "unknown"] = by_device.get(payload_device or "unknown", 0) + 1
            first_step = mapped_steps[matched_positions[0]]
            first_group = "paid" if first_step == STEP_PAID_LANDING else "organic"
            by_channel[first_group] = by_channel.get(first_group, 0) + 1
        if len(matched_positions) >= 2:
            for idx in range(1, len(matched_positions)):
                s1 = steps[idx - 1]
                s2 = steps[idx]
                t1 = seq[matched_positions[idx - 1]][1]
                t2 = seq[matched_positions[idx]][1]
                delta = (t2 - t1).total_seconds()
                if delta >= 0:
                    pair_times[(s1, s2)].append(delta)

    time_between = []
    for a, b in zip(steps, steps[1:]):
        vals = pair_times.get((a, b), [])
        if not vals:
            continue
        avg = sum(vals) / len(vals)
        time_between.append(
            {
                "from_step": a,
                "to_step": b,
                "count": len(vals),
                "avg_sec": round(avg, 2),
                "p50_sec": round(_percentile(vals, 0.5) or 0.0, 2),
                "p90_sec": round(_percentile(vals, 0.9) or 0.0, 2),
            }
        )

    device_breakdown = [{"key": k, "count": v} for k, v in sorted(by_device.items(), key=lambda x: x[1], reverse=True)[:5]]
    channel_breakdown = [{"key": k, "count": v} for k, v in sorted(by_channel.items(), key=lambda x: x[1], reverse=True)[:5]]

    return {
        "step_counts": step_counts,
        "time_between": time_between,
        "breakdown_device": device_breakdown,
        "breakdown_channel_group": channel_breakdown,
        "source": "raw",
    }


def get_funnel_results(
    db: Session,
    *,
    funnel: FunnelDefinition,
    journey_definition: JourneyDefinition,
    date_from: date,
    date_to: date,
    device: Optional[str] = None,
    channel_group: Optional[str] = None,
    country: Optional[str] = None,
    campaign_id: Optional[str] = None,
) -> Dict[str, Any]:
    steps = [str(s).strip() for s in (funnel.steps_json or []) if str(s).strip()]
    if len(steps) < 2:
        return {
            "funnel_id": funnel.id,
            "steps": [],
            "time_between_steps": [],
            "breakdown": {"device": [], "channel_group": []},
            "meta": {
                "source": "none",
                "used_raw_fallback": False,
                "warning": "Funnel requires at least 2 steps.",
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
            },
        }

    agg = _compute_results_from_transitions(
        db,
        journey_definition_id=funnel.journey_definition_id,
        steps=steps,
        date_from=date_from,
        date_to=date_to,
        device=device,
        channel_group=channel_group,
        country=country,
        campaign_id=campaign_id,
    )
    raw = None
    warning = None
    source = "aggregates"
    used_raw = False
    if not agg:
        raw = _compute_results_from_raw(
            db,
            journey_definition=journey_definition,
            steps=steps,
            date_from=date_from,
            date_to=date_to,
            device=device,
            channel_group=channel_group,
            country=country,
            campaign_id=campaign_id,
        )
        agg = raw
        source = "raw"
        used_raw = True
        warning = "Transitions aggregates unavailable for this funnel/date range. Results computed from raw conversion paths."
    else:
        raw_timing = _compute_results_from_raw(
            db,
            journey_definition=journey_definition,
            steps=steps,
            date_from=date_from,
            date_to=date_to,
            device=device,
            channel_group=channel_group,
            country=country,
            campaign_id=campaign_id,
        )
        if raw_timing["time_between"]:
            agg["time_between"] = raw_timing["time_between"]
            source = "mixed"
            used_raw = True
            warning = "Step timings are computed from raw conversion paths because transition aggregates do not include timing metrics."

    step_counts = agg["step_counts"]
    steps_out = []
    for idx, step in enumerate(steps):
        count = int(step_counts[idx]) if idx < len(step_counts) else 0
        next_count = int(step_counts[idx + 1]) if idx + 1 < len(step_counts) else None
        dropoff_pct = ((count - next_count) / count) if (next_count is not None and count > 0) else None
        steps_out.append(
            {
                "step": step,
                "position": idx + 1,
                "count": count,
                "dropoff_pct": round(dropoff_pct, 4) if dropoff_pct is not None else None,
            }
        )

    return {
        "funnel_id": funnel.id,
        "steps": steps_out,
        "time_between_steps": agg["time_between"],
        "breakdown": {
            "device": agg["breakdown_device"],
            "channel_group": agg["breakdown_channel_group"],
        },
        "meta": {
            "source": source,
            "used_raw_fallback": used_raw,
            "warning": warning,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
        },
    }


def _extract_browser(payload: Dict[str, Any]) -> Optional[str]:
    cand = payload.get("browser")
    if not cand and isinstance(payload.get("context"), dict):
        cand = payload["context"].get("browser")
    if cand:
        out = str(cand).strip().lower()
        return out or None
    tps = payload.get("touchpoints") or []
    if isinstance(tps, list):
        for tp in tps:
            if isinstance(tp, dict):
                c = tp.get("browser") or (tp.get("context") or {}).get("browser") if isinstance(tp.get("context"), dict) else tp.get("browser")
                if c:
                    out = str(c).strip().lower()
                    if out:
                        return out
    return None


def _extract_consent_opt_out(payload: Dict[str, Any]) -> Optional[bool]:
    if "consent_opt_out" in payload:
        return bool(payload.get("consent_opt_out"))
    consent = payload.get("consent")
    if isinstance(consent, dict):
        if "opt_out" in consent:
            return bool(consent.get("opt_out"))
        if "tracking" in consent:
            return not bool(consent.get("tracking"))
    tps = payload.get("touchpoints") or []
    if isinstance(tps, list):
        for tp in tps:
            if not isinstance(tp, dict):
                continue
            if "consent_opt_out" in tp:
                return bool(tp.get("consent_opt_out"))
    return None


def _extract_landing_group(payload: Dict[str, Any]) -> Optional[str]:
    for key in ("landing_page_group", "landing_group", "page_group"):
        if payload.get(key):
            return str(payload.get(key)).strip().lower() or None
    tps = payload.get("touchpoints") or []
    if isinstance(tps, list) and tps:
        tp0 = tps[0] if isinstance(tps[0], dict) else {}
        for key in ("landing_page_group", "landing_group", "page_group"):
            if tp0.get(key):
                return str(tp0.get(key)).strip().lower() or None
        url = tp0.get("url") or tp0.get("landing_page")
        if url:
            s = str(url).strip().lower()
            if "pricing" in s:
                return "pricing"
            if "blog" in s or "content" in s:
                return "content"
            if "product" in s:
                return "product"
            return "other"
    return None


def _has_error_event(payload: Dict[str, Any]) -> Optional[bool]:
    tps = payload.get("touchpoints") or []
    if not isinstance(tps, list):
        return None
    seen = False
    for tp in tps:
        if not isinstance(tp, dict):
            continue
        seen = True
        ev = str(tp.get("event") or tp.get("event_name") or tp.get("name") or tp.get("type") or "").lower()
        if any(tok in ev for tok in ("error", "fail", "exception", "timeout")):
            return True
    return False if seen else None


def _previous_period(date_from: date, date_to: date) -> Tuple[date, date]:
    length = (date_to - date_from).days + 1
    prev_to = date_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=length - 1)
    return prev_from, prev_to


def _top_share(counts: Dict[str, int]) -> Tuple[Optional[str], float, int]:
    if not counts:
        return None, 0.0, 0
    total = sum(counts.values())
    if total <= 0:
        return None, 0.0, 0
    key, val = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[0]
    return key, (val / total), total


def _safe_pct(delta: float) -> float:
    return round(delta * 100.0, 1)


def _cohort_metrics_for_step(
    db: Session,
    *,
    journey_definition: JourneyDefinition,
    steps: Sequence[str],
    step_index: int,
    date_from: date,
    date_to: date,
    device: Optional[str],
    channel_group: Optional[str],
    country: Optional[str],
    campaign_id: Optional[str],
) -> Dict[str, Any]:
    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    q = db.query(ConversionPath).filter(ConversionPath.conversion_ts >= start_dt, ConversionPath.conversion_ts < end_dt)
    if journey_definition.conversion_kpi_id:
        q = q.filter(ConversionPath.conversion_key == journey_definition.conversion_kpi_id)
    rows = q.all()

    reached = 0
    advanced = 0
    times_to_next: List[float] = []
    device_counts: Dict[str, int] = {}
    browser_counts: Dict[str, int] = {}
    geo_counts: Dict[str, int] = {}
    landing_counts: Dict[str, int] = {}
    consent_known = 0
    consent_opt_out = 0
    error_known = 0
    error_true = 0

    for row in rows:
        payload = row.path_json if isinstance(row.path_json, dict) else {}
        payload_device = str(payload.get("device") or "").strip().lower()
        payload_country = str(payload.get("country") or "").strip().upper()
        if device and payload_device and payload_device != device.lower():
            continue
        if country and payload_country and payload_country != country.upper():
            continue
        seq = _extract_steps_with_ts(payload, row.conversion_ts)
        if not seq:
            continue
        mapped_steps = [s for s, _ in seq]
        if channel_group:
            first = mapped_steps[0] if mapped_steps else ""
            if channel_group == "paid" and first != STEP_PAID_LANDING:
                continue
            if channel_group == "organic" and first != STEP_ORGANIC_LANDING:
                continue
        if campaign_id:
            tps = payload.get("touchpoints") or []
            last_campaign = None
            if isinstance(tps, list) and tps:
                cand = tps[-1].get("campaign") if isinstance(tps[-1], dict) else None
                if isinstance(cand, dict):
                    last_campaign = cand.get("id") or cand.get("name")
                else:
                    last_campaign = cand
            if str(last_campaign or "") != campaign_id:
                continue

        matched_idx = -1
        matched_positions: List[int] = []
        for target in steps:
            found = None
            for pos in range(matched_idx + 1, len(mapped_steps)):
                if mapped_steps[pos] == target:
                    found = pos
                    break
            if found is None:
                break
            matched_idx = found
            matched_positions.append(found)

        if len(matched_positions) <= step_index:
            continue
        reached += 1

        device_key = payload_device or "unknown"
        device_counts[device_key] = device_counts.get(device_key, 0) + 1
        geo_key = payload_country or "unknown"
        geo_counts[geo_key] = geo_counts.get(geo_key, 0) + 1
        browser_key = _extract_browser(payload) or "unknown"
        browser_counts[browser_key] = browser_counts.get(browser_key, 0) + 1
        landing_key = _extract_landing_group(payload)
        if landing_key:
            landing_counts[landing_key] = landing_counts.get(landing_key, 0) + 1

        consent = _extract_consent_opt_out(payload)
        if consent is not None:
            consent_known += 1
            if consent:
                consent_opt_out += 1
        err = _has_error_event(payload)
        if err is not None:
            error_known += 1
            if err:
                error_true += 1

        if len(matched_positions) > step_index + 1:
            advanced += 1
            from_pos = matched_positions[step_index]
            to_pos = matched_positions[step_index + 1]
            delta = (seq[to_pos][1] - seq[from_pos][1]).total_seconds()
            if delta >= 0:
                times_to_next.append(delta)

    dropoff = ((reached - advanced) / reached) if reached > 0 else None
    return {
        "reached": reached,
        "advanced": advanced,
        "dropoff_pct": round(dropoff, 4) if dropoff is not None else None,
        "time_next_count": len(times_to_next),
        "time_next_avg_sec": round(sum(times_to_next) / len(times_to_next), 2) if times_to_next else None,
        "time_next_p50_sec": round(_percentile(times_to_next, 0.5) or 0.0, 2) if times_to_next else None,
        "time_next_p90_sec": round(_percentile(times_to_next, 0.9) or 0.0, 2) if times_to_next else None,
        "device_counts": device_counts,
        "browser_counts": browser_counts,
        "geo_counts": geo_counts,
        "landing_counts": landing_counts,
        "consent_known": consent_known,
        "consent_opt_out": consent_opt_out,
        "error_known": error_known,
        "error_true": error_true,
    }


def get_funnel_diagnostics(
    db: Session,
    *,
    funnel: FunnelDefinition,
    journey_definition: JourneyDefinition,
    step: str,
    date_from: date,
    date_to: date,
    device: Optional[str] = None,
    channel_group: Optional[str] = None,
    country: Optional[str] = None,
    campaign_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    steps = [str(s).strip() for s in (funnel.steps_json or []) if str(s).strip()]
    if len(steps) < 2:
        return [
            {
                "title": "Insufficient evidence for funnel diagnostics",
                "evidence": ["Funnel has fewer than 2 configured steps, so drop-off diagnostics cannot be computed."],
                "impact_estimate": {"direction": "unknown", "magnitude": "n/a"},
                "confidence": "low",
                "next_action": "Update the funnel definition to include at least two ordered steps.",
            }
        ]

    try:
        step_idx = steps.index(step)
    except ValueError:
        return [
            {
                "title": "Insufficient evidence for funnel diagnostics",
                "evidence": [f"Requested step '{step}' is not present in this funnel definition."],
                "impact_estimate": {"direction": "unknown", "magnitude": "n/a"},
                "confidence": "low",
                "next_action": "Use a step value that exists in the funnel definition.",
            }
        ]
    if step_idx >= len(steps) - 1:
        return [
            {
                "title": "Insufficient evidence for funnel diagnostics",
                "evidence": [f"Step '{step}' is the last step, so there is no next-step drop-off to diagnose."],
                "impact_estimate": {"direction": "unknown", "magnitude": "n/a"},
                "confidence": "low",
                "next_action": "Choose an intermediate step to diagnose drop-offs.",
            }
        ]

    prev_from, prev_to = _previous_period(date_from, date_to)
    curr = _cohort_metrics_for_step(
        db,
        journey_definition=journey_definition,
        steps=steps,
        step_index=step_idx,
        date_from=date_from,
        date_to=date_to,
        device=device,
        channel_group=channel_group,
        country=country,
        campaign_id=campaign_id,
    )
    prev = _cohort_metrics_for_step(
        db,
        journey_definition=journey_definition,
        steps=steps,
        step_index=step_idx,
        date_from=prev_from,
        date_to=prev_to,
        device=device,
        channel_group=channel_group,
        country=country,
        campaign_id=campaign_id,
    )

    hypotheses: List[Dict[str, Any]] = []
    next_step = steps[step_idx + 1]
    curr_drop = float(curr["dropoff_pct"] or 0.0)
    prev_drop = float(prev["dropoff_pct"] or 0.0)
    drop_delta = curr_drop - prev_drop
    affected = int(round(max(0.0, drop_delta) * max(0, curr["reached"])))

    # 1) Time-to-next-step spike
    curr_p90 = curr.get("time_next_p90_sec")
    prev_p90 = prev.get("time_next_p90_sec")
    if curr_p90 is not None and prev_p90 is not None and prev_p90 > 0:
        ratio = curr_p90 / prev_p90
        if ratio >= 1.2:
            conf = "high" if curr["time_next_count"] >= 40 else "medium"
            hypotheses.append(
                {
                    "title": f"Hypothesis: latency increased between '{step}' and '{next_step}'",
                    "evidence": [
                        (
                            f"P90 time-to-next-step is {curr_p90:.0f}s for {date_from.isoformat()} to {date_to.isoformat()} "
                            f"vs {prev_p90:.0f}s for {prev_from.isoformat()} to {prev_to.isoformat()} "
                            f"({_safe_pct(ratio - 1.0):+.1f}%)."
                        ),
                        (
                            f"Step drop-off is {curr_drop*100:.1f}% vs {prev_drop*100:.1f}% "
                            f"({_safe_pct(drop_delta):+.1f}pp), reached users: {curr['reached']} vs {prev['reached']}."
                        ),
                    ],
                    "impact_estimate": {
                        "direction": "negative" if drop_delta > 0 else "neutral",
                        "estimated_users_affected": affected,
                    },
                    "confidence": conf,
                    "next_action": "Inspect page/app latency and instrumented wait times specifically between these two steps.",
                }
            )

    # 2) Device/browser/geo skew changes
    for signal_key, label in (
        ("device_counts", "device"),
        ("browser_counts", "browser"),
        ("geo_counts", "geo"),
    ):
        curr_top, curr_share, curr_total = _top_share(curr.get(signal_key, {}) or {})
        prev_top, prev_share, prev_total = _top_share(prev.get(signal_key, {}) or {})
        if curr_top and prev_top and curr_total >= 20 and prev_total >= 20:
            share_delta = curr_share - prev_share if curr_top == prev_top else curr_share
            if abs(share_delta) >= 0.1 or curr_top != prev_top:
                conf = "high" if min(curr_total, prev_total) >= 80 else "medium"
                hypotheses.append(
                    {
                        "title": f"Hypothesis: {label} mix shift may be contributing to drop-off",
                        "evidence": [
                            (
                                f"Top {label} in current period is '{curr_top}' at {curr_share*100:.1f}% "
                                f"({curr_total} reached users) vs "
                                f"{prev_top!r} at {prev_share*100:.1f}% ({prev_total} reached users) in previous period."
                            ),
                            (
                                f"Step drop-off is {curr_drop*100:.1f}% vs {prev_drop*100:.1f}% "
                                f"({_safe_pct(drop_delta):+.1f}pp) over the same periods."
                            ),
                        ],
                        "impact_estimate": {
                            "direction": "negative" if drop_delta > 0 else "unclear",
                            "estimated_users_affected": affected,
                        },
                        "confidence": conf,
                        "next_action": f"Break down conversion path performance by {label} for this funnel transition and check UX parity.",
                    }
                )

    # 3) Consent opt-out change (if tracked)
    if curr["consent_known"] > 0 and prev["consent_known"] > 0:
        curr_rate = curr["consent_opt_out"] / max(1, curr["consent_known"])
        prev_rate = prev["consent_opt_out"] / max(1, prev["consent_known"])
        if abs(curr_rate - prev_rate) >= 0.03:
            hypotheses.append(
                {
                    "title": "Hypothesis: consent opt-out shift may be reducing trackable progression",
                    "evidence": [
                        (
                            f"Consent opt-out rate is {curr_rate*100:.1f}% ({curr['consent_opt_out']}/{curr['consent_known']}) "
                            f"vs {prev_rate*100:.1f}% ({prev['consent_opt_out']}/{prev['consent_known']}) in previous period."
                        ),
                        (
                            f"Observed drop-off difference at step is {_safe_pct(drop_delta):+.1f}pp "
                            f"({curr_drop*100:.1f}% vs {prev_drop*100:.1f}%)."
                        ),
                    ],
                    "impact_estimate": {"direction": "unclear", "estimated_users_affected": affected},
                    "confidence": "medium",
                    "next_action": "Review consent banner/version changes and validate server-side events for opted-out users where allowed.",
                }
            )

    # 4) Error event change (if tracked)
    if curr["error_known"] > 0 and prev["error_known"] > 0:
        curr_rate = curr["error_true"] / max(1, curr["error_known"])
        prev_rate = prev["error_true"] / max(1, prev["error_known"])
        if abs(curr_rate - prev_rate) >= 0.03:
            hypotheses.append(
                {
                    "title": "Hypothesis: higher error-event rate may be causing step exits",
                    "evidence": [
                        (
                            f"Error-event rate in step cohort is {curr_rate*100:.1f}% ({curr['error_true']}/{curr['error_known']}) "
                            f"vs {prev_rate*100:.1f}% ({prev['error_true']}/{prev['error_known']}) in previous period."
                        ),
                        (
                            f"Drop-off at this step is {curr_drop*100:.1f}% vs {prev_drop*100:.1f}% "
                            f"({_safe_pct(drop_delta):+.1f}pp)."
                        ),
                    ],
                    "impact_estimate": {"direction": "negative" if curr_rate > prev_rate else "unclear", "estimated_users_affected": affected},
                    "confidence": "medium",
                    "next_action": "Check error logs and client-side exception traces scoped to this funnel transition.",
                }
            )

    # 5) Landing-page-group change (if tracked)
    curr_top, curr_share, curr_total = _top_share(curr.get("landing_counts", {}) or {})
    prev_top, prev_share, prev_total = _top_share(prev.get("landing_counts", {}) or {})
    if curr_top and prev_top and curr_total >= 20 and prev_total >= 20:
        if curr_top != prev_top or abs(curr_share - prev_share) >= 0.1:
            hypotheses.append(
                {
                    "title": "Hypothesis: landing page mix shift may be impacting downstream progression",
                    "evidence": [
                        (
                            f"Top landing-page group is '{curr_top}' at {curr_share*100:.1f}% "
                            f"({curr_total} reached users) vs '{prev_top}' at {prev_share*100:.1f}% "
                            f"({prev_total} reached users) in previous period."
                        ),
                        (
                            f"Drop-off difference at step is {_safe_pct(drop_delta):+.1f}pp "
                            f"({curr_drop*100:.1f}% vs {prev_drop*100:.1f}%)."
                        ),
                    ],
                    "impact_estimate": {"direction": "unclear", "estimated_users_affected": affected},
                    "confidence": "medium",
                    "next_action": "Compare message-match and UX quality across landing page groups for this transition.",
                }
            )

    if not hypotheses:
        hypotheses.append(
            {
                "title": "Insufficient evidence for a specific drop-off driver",
                "evidence": [
                    (
                        f"Current drop-off at '{step}' → '{next_step}' is {curr_drop*100:.1f}% "
                        f"({curr['reached']} reached, {curr['advanced']} advanced) vs "
                        f"{prev_drop*100:.1f}% ({prev['reached']} reached, {prev['advanced']} advanced) "
                        f"in {prev_from.isoformat()} to {prev_to.isoformat()}."
                    ),
                    (
                        "Available optional signals (browser, consent opt-out, error events, landing page group) "
                        "did not show statistically meaningful movement or were not tracked sufficiently."
                    ),
                ],
                "impact_estimate": {"direction": "unclear", "estimated_users_affected": affected},
                "confidence": "low",
                "next_action": "Increase sample size or enrich instrumentation for browser/consent/error/landing signals to improve diagnosis confidence.",
            }
        )

    return hypotheses
