"""Read API helpers for journey transition Sankey/flow views."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import JourneyDefinition, JourneyTransitionDaily
from .services_journey_transition_facts import iter_journey_transition_rows

OTHER_STEP = "Other"
_STEP_ORDER = {
    "Paid Landing": 1,
    "Organic Landing": 1,
    "Product View / Content View": 2,
    "Add to Cart / Form Start": 3,
    "Checkout / Form Submit": 4,
    "Purchase / Lead Won (conversion)": 5,
}


def _step_depth(step: str) -> int:
    return _STEP_ORDER.get(step, 99)


def _aggregate_edges(
    db: Session,
    *,
    journey_definition: JourneyDefinition,
    date_from: date,
    date_to: date,
    channel_group: Optional[str],
    campaign_id: Optional[str],
    device: Optional[str],
    country: Optional[str],
) -> List[Dict[str, Any]]:
    q = (
        db.query(
            JourneyTransitionDaily.from_step.label("from_step"),
            JourneyTransitionDaily.to_step.label("to_step"),
            func.sum(JourneyTransitionDaily.count_transitions).label("count_transitions"),
            func.sum(JourneyTransitionDaily.count_profiles).label("count_profiles"),
            func.sum(JourneyTransitionDaily.avg_time_between_sec * JourneyTransitionDaily.count_transitions).label("weighted_avg_time_between_sec"),
            func.sum(JourneyTransitionDaily.p50_time_between_sec * JourneyTransitionDaily.count_transitions).label("weighted_p50_time_between_sec"),
            func.sum(JourneyTransitionDaily.p90_time_between_sec * JourneyTransitionDaily.count_transitions).label("weighted_p90_time_between_sec"),
        )
        .filter(
            JourneyTransitionDaily.journey_definition_id == journey_definition.id,
            JourneyTransitionDaily.date >= date_from,
            JourneyTransitionDaily.date <= date_to,
        )
        .group_by(JourneyTransitionDaily.from_step, JourneyTransitionDaily.to_step)
    )
    if channel_group:
        q = q.filter(JourneyTransitionDaily.channel_group == channel_group)
    if campaign_id:
        q = q.filter(JourneyTransitionDaily.campaign_id == campaign_id)
    if device:
        q = q.filter(JourneyTransitionDaily.device == device)
    if country:
        q = q.filter(JourneyTransitionDaily.country == country)
    rows = q.all()
    return [
        {
            "from_step": str(r.from_step),
            "to_step": str(r.to_step),
            "count_transitions": int(r.count_transitions or 0),
            "count_profiles": int(r.count_profiles or 0),
            "avg_time_between_sec": (
                round(float(r.weighted_avg_time_between_sec or 0.0) / float(r.count_transitions or 1), 2)
                if int(r.count_transitions or 0) > 0 and r.weighted_avg_time_between_sec is not None
                else None
            ),
            "p50_time_between_sec": (
                round(float(r.weighted_p50_time_between_sec or 0.0) / float(r.count_transitions or 1), 2)
                if int(r.count_transitions or 0) > 0 and r.weighted_p50_time_between_sec is not None
                else None
            ),
            "p90_time_between_sec": (
                round(float(r.weighted_p90_time_between_sec or 0.0) / float(r.count_transitions or 1), 2)
                if int(r.count_transitions or 0) > 0 and r.weighted_p90_time_between_sec is not None
                else None
            ),
        }
        for r in rows
        if int(r.count_transitions or 0) > 0
    ]


def _can_fallback_to_transition_facts(db: Session, *, journey_definition: JourneyDefinition) -> bool:
    conversion_key = str(journey_definition.conversion_kpi_id or "").strip()
    if not conversion_key:
        return False
    matching_definition_ids = [
        str(definition_id or "")
        for (definition_id,) in db.query(JourneyDefinition.id)
        .filter(
            JourneyDefinition.is_archived == False,  # noqa: E712
            JourneyDefinition.conversion_kpi_id == conversion_key,
        )
        .all()
    ]
    return matching_definition_ids == [journey_definition.id]


def _aggregate_edges_from_transition_facts(
    db: Session,
    *,
    journey_definition: JourneyDefinition,
    date_from: date,
    date_to: date,
    channel_group: Optional[str],
    campaign_id: Optional[str],
    device: Optional[str],
    country: Optional[str],
) -> List[Dict[str, Any]]:
    if not _can_fallback_to_transition_facts(db, journey_definition=journey_definition):
        return []

    start_dt = datetime.combine(date_from, dt_time.min)
    end_dt = datetime.combine(date_to + timedelta(days=1), dt_time.min)
    buckets: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in iter_journey_transition_rows(
        db,
        start_dt=start_dt,
        end_dt=end_dt,
        conversion_key=journey_definition.conversion_kpi_id,
    ):
        if channel_group and str(row.channel_group or "") != channel_group:
            continue
        if campaign_id and str(row.campaign_id or "") != campaign_id:
            continue
        if device and str(row.device or "") != device:
            continue
        if country and str(row.country or "") != country:
            continue
        key = (str(row.from_step or ""), str(row.to_step or ""))
        bucket = buckets.setdefault(
            key,
            {
                "from_step": key[0],
                "to_step": key[1],
                "count_transitions": 0,
                "profiles": set(),
                "time_values": [],
            },
        )
        bucket["count_transitions"] += 1
        if row.profile_id:
            bucket["profiles"].add(str(row.profile_id))
        if isinstance(row.delta_sec, (int, float)) and float(row.delta_sec) >= 0:
            bucket["time_values"].append(float(row.delta_sec))

    return [
        {
            "from_step": payload["from_step"],
            "to_step": payload["to_step"],
            "count_transitions": int(payload["count_transitions"]),
            "count_profiles": len(payload["profiles"]),
            "avg_time_between_sec": round(sum(payload["time_values"]) / len(payload["time_values"]), 2) if payload["time_values"] else None,
            "p50_time_between_sec": None,
            "p90_time_between_sec": None,
        }
        for payload in buckets.values()
        if int(payload["count_transitions"] or 0) > 0
    ]


def list_transitions_for_journey_definition(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
    mode: str = "conversion_only",
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
    min_count: int = 5,
    max_nodes: int = 20,
    max_depth: int = 5,
    group_other: bool = True,
) -> Dict[str, Any]:
    journey_definition = db.get(JourneyDefinition, journey_definition_id)
    if journey_definition is None:
        return {
            "nodes": [],
            "edges": [],
            "meta": {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "mode": mode,
                "min_count": max(1, int(min_count)),
                "max_nodes": max(2, min(int(max_nodes), 200)),
                "max_depth": max(1, min(int(max_depth), 20)),
                "grouped_to_other": False,
                "dropped_edges": 0,
                "source": "none",
            },
        }
    # mode is kept for compatibility with /paths filter semantics.
    min_count = max(1, int(min_count))
    max_nodes = max(2, min(int(max_nodes), 200))
    max_depth = max(1, min(int(max_depth), 20))

    edges_raw = _aggregate_edges(
        db,
        journey_definition=journey_definition,
        date_from=date_from,
        date_to=date_to,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
    )
    source = "aggregates"
    if not edges_raw:
        edges_raw = _aggregate_edges_from_transition_facts(
            db,
            journey_definition=journey_definition,
            date_from=date_from,
            date_to=date_to,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
        )
        source = "transition_facts" if edges_raw else "none"

    edges_depth = [
        e
        for e in edges_raw
        if _step_depth(e["from_step"]) <= max_depth and _step_depth(e["to_step"]) <= max_depth
    ]
    edges_filtered = [e for e in edges_depth if e["count_transitions"] >= min_count]

    if not edges_filtered:
        return {
            "nodes": [],
            "edges": [],
            "meta": {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "mode": mode,
                "min_count": min_count,
                "max_nodes": max_nodes,
                "max_depth": max_depth,
                "grouped_to_other": False,
                "dropped_edges": len(edges_raw),
                "source": source,
            },
        }

    node_weight: Dict[str, int] = defaultdict(int)
    for e in edges_filtered:
        node_weight[e["from_step"]] += e["count_transitions"]
        node_weight[e["to_step"]] += e["count_transitions"]

    nodes_sorted = sorted(
        node_weight.keys(),
        key=lambda step: (_step_depth(step), -node_weight[step], step),
    )
    keep_nodes: Set[str] = set(nodes_sorted[:max_nodes])
    grouped_to_other = len(nodes_sorted) > len(keep_nodes)

    remapped: Dict[Tuple[str, str], Dict[str, float]] = {}
    for e in edges_filtered:
        src = e["from_step"] if e["from_step"] in keep_nodes else (OTHER_STEP if group_other else "")
        tgt = e["to_step"] if e["to_step"] in keep_nodes else (OTHER_STEP if group_other else "")
        if not src or not tgt:
            continue
        if src == tgt:
            continue
        key = (src, tgt)
        bucket = remapped.setdefault(
            key,
            {
                "count_transitions": 0.0,
                "count_profiles": 0.0,
                "weighted_avg_time_between_sec": 0.0,
                "weighted_p50_time_between_sec": 0.0,
                "weighted_p90_time_between_sec": 0.0,
                "timing_weight": 0.0,
            },
        )
        bucket["count_transitions"] += e["count_transitions"]
        bucket["count_profiles"] += e["count_profiles"]
        if e.get("avg_time_between_sec") is not None:
            weight = float(e["count_transitions"] or 0.0)
            bucket["weighted_avg_time_between_sec"] += float(e["avg_time_between_sec"]) * weight
            bucket["weighted_p50_time_between_sec"] += float(e["p50_time_between_sec"] or 0.0) * weight
            bucket["weighted_p90_time_between_sec"] += float(e["p90_time_between_sec"] or 0.0) * weight
            bucket["timing_weight"] += weight

    final_edges = [
        {
            "source": src,
            "target": tgt,
            "value": int(vals["count_transitions"]),
            "count_transitions": int(vals["count_transitions"]),
            "count_profiles": int(vals["count_profiles"]),
            "avg_time_between_sec": round(vals["weighted_avg_time_between_sec"] / vals["timing_weight"], 2) if vals["timing_weight"] > 0 else None,
            "p50_time_between_sec": round(vals["weighted_p50_time_between_sec"] / vals["timing_weight"], 2) if vals["timing_weight"] > 0 else None,
            "p90_time_between_sec": round(vals["weighted_p90_time_between_sec"] / vals["timing_weight"], 2) if vals["timing_weight"] > 0 else None,
        }
        for (src, tgt), vals in remapped.items()
        if vals["count_transitions"] >= min_count
    ]
    final_edges.sort(key=lambda r: (-r["value"], r["source"], r["target"]))

    connected: Set[str] = set()
    for e in final_edges:
        connected.add(e["source"])
        connected.add(e["target"])

    node_in: Dict[str, int] = defaultdict(int)
    node_out: Dict[str, int] = defaultdict(int)
    for e in final_edges:
        node_out[e["source"]] += e["value"]
        node_in[e["target"]] += e["value"]

    ordered_nodes = sorted(connected, key=lambda step: (_step_depth(step), step != OTHER_STEP, step))
    nodes = [
        {
            "id": step,
            "label": step,
            "depth": None if step == OTHER_STEP else _step_depth(step),
            "count_in": node_in.get(step, 0),
            "count_out": node_out.get(step, 0),
            "count_total": node_in.get(step, 0) + node_out.get(step, 0),
        }
        for step in ordered_nodes
    ]

    return {
        "nodes": nodes,
        "edges": final_edges,
        "meta": {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "mode": mode,
            "min_count": min_count,
            "max_nodes": max_nodes,
            "max_depth": max_depth,
            "group_other": bool(group_other),
            "grouped_to_other": grouped_to_other,
            "dropped_edges": max(0, len(edges_raw) - len(final_edges)),
            "source": source,
        },
    }
