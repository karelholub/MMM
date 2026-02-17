"""Compatibility adapter: build Conversion Paths analysis from journey_paths_daily aggregates."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import JourneyDefinition, JourneyPathDaily


def _steps_from_value(path_steps: Any) -> List[str]:
    if isinstance(path_steps, list):
        return [str(s).strip() for s in path_steps if str(s).strip()]
    if isinstance(path_steps, str):
        return [s.strip() for s in path_steps.split(">") if s.strip()]
    return []


def _is_direct_unknown(step: str) -> bool:
    token = step.strip().lower().split(":", 1)[0]
    return token in {"direct", "unknown"}


def _apply_direct_mode(steps: Sequence[str], direct_mode: str) -> List[str]:
    if direct_mode != "exclude":
        return list(steps)
    return [s for s in steps if not _is_direct_unknown(s)]


def _weighted_quantile(counts_by_value: Dict[int, int], q: float) -> int:
    if not counts_by_value:
        return 0
    q = min(1.0, max(0.0, q))
    total = sum(max(0, c) for c in counts_by_value.values())
    if total <= 0:
        return 0
    threshold = total * q
    running = 0
    for value in sorted(counts_by_value):
        running += max(0, counts_by_value[value])
        if running >= threshold:
            return int(value)
    return int(max(counts_by_value))


def _resolve_definition(db: Session, definition_id: Optional[str]) -> Optional[JourneyDefinition]:
    if definition_id:
        row = db.get(JourneyDefinition, definition_id)
        if row and not row.is_archived:
            return row
        return None
    return (
        db.query(JourneyDefinition)
        .filter(JourneyDefinition.is_archived == False)  # noqa: E712
        .order_by(JourneyDefinition.updated_at.desc())
        .first()
    )


def _resolve_date_bounds(
    db: Session,
    *,
    definition_id: str,
    date_from: Optional[date],
    date_to: Optional[date],
) -> Tuple[Optional[date], Optional[date]]:
    if date_from and date_to:
        return date_from, date_to
    q = db.query(func.min(JourneyPathDaily.date), func.max(JourneyPathDaily.date)).filter(
        JourneyPathDaily.journey_definition_id == definition_id
    )
    min_date, max_date = q.first() or (None, None)
    if min_date is None or max_date is None:
        return None, None
    return date_from or min_date, date_to or max_date


def _query_rows(
    db: Session,
    *,
    definition_id: str,
    date_from: date,
    date_to: date,
    mode: str,
    channel_group: Optional[str],
    campaign_id: Optional[str],
    device: Optional[str],
    country: Optional[str],
) -> List[JourneyPathDaily]:
    q = db.query(JourneyPathDaily).filter(
        JourneyPathDaily.journey_definition_id == definition_id,
        JourneyPathDaily.date >= date_from,
        JourneyPathDaily.date <= date_to,
    )
    if channel_group:
        q = q.filter(JourneyPathDaily.channel_group == channel_group)
    if campaign_id:
        q = q.filter(JourneyPathDaily.campaign_id == campaign_id)
    if device:
        q = q.filter(JourneyPathDaily.device == device)
    if country:
        q = q.filter(JourneyPathDaily.country == country)
    if mode == "conversion_only":
        q = q.filter(JourneyPathDaily.count_conversions > 0)
    return q.all()


def build_conversion_paths_analysis_from_daily(
    db: Session,
    *,
    definition_id: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    direct_mode: str = "include",
    path_scope: str = "converted",
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
    nba_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    definition = _resolve_definition(db, definition_id)
    if not definition:
        return {
            "total_journeys": 0,
            "avg_path_length": 0,
            "avg_time_to_conversion_days": None,
            "common_paths": [],
            "channel_frequency": {},
            "path_length_distribution": {"min": 0, "max": 0, "median": 0, "p90": 0},
            "time_to_conversion_distribution": None,
            "direct_unknown_diagnostics": {"touchpoint_share": 0.0, "journeys_ending_direct_share": 0.0},
            "config": None,
            "view_filters": {"direct_mode": direct_mode, "path_scope": path_scope},
            "nba_config": nba_config or {},
            "next_best_by_prefix": {},
            "next_best_by_prefix_campaign": {},
            "source": "journey_paths_daily",
        }

    mode = "all_journeys" if (path_scope or "converted").lower() in {"all", "all_journeys"} else "conversion_only"
    start_d, end_d = _resolve_date_bounds(db, definition_id=definition.id, date_from=date_from, date_to=date_to)
    if start_d is None or end_d is None:
        return {
            "total_journeys": 0,
            "avg_path_length": 0,
            "avg_time_to_conversion_days": None,
            "common_paths": [],
            "channel_frequency": {},
            "path_length_distribution": {"min": 0, "max": 0, "median": 0, "p90": 0},
            "time_to_conversion_distribution": None,
            "direct_unknown_diagnostics": {"touchpoint_share": 0.0, "journeys_ending_direct_share": 0.0},
            "config": None,
            "view_filters": {"direct_mode": direct_mode, "path_scope": "all" if mode == "all_journeys" else "converted"},
            "nba_config": nba_config or {},
            "next_best_by_prefix": {},
            "next_best_by_prefix_campaign": {},
            "source": "journey_paths_daily",
        }

    rows = _query_rows(
        db,
        definition_id=definition.id,
        date_from=start_d,
        date_to=end_d,
        mode=mode,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
    )

    aggregated: Dict[Tuple[str, ...], Dict[str, float]] = defaultdict(lambda: {
        "count_journeys": 0.0,
        "count_conversions": 0.0,
        "ttc_weighted_sec": 0.0,
        "ttc_weight": 0.0,
    })
    channel_frequency: Dict[str, int] = defaultdict(int)
    path_len_counts: Dict[int, int] = defaultdict(int)
    ttc_bucket_days: Dict[int, int] = defaultdict(int)
    total_touchpoints = 0
    direct_touchpoints = 0
    journeys_ending_direct = 0

    for row in rows:
        raw_steps = _steps_from_value(row.path_steps)
        steps = _apply_direct_mode(raw_steps, direct_mode)
        if not steps:
            continue
        key = tuple(steps)
        cj = int(row.count_journeys or 0)
        cc = int(row.count_conversions or 0)
        aggregated[key]["count_journeys"] += cj
        aggregated[key]["count_conversions"] += cc

        if row.avg_time_to_convert_sec is not None and cc > 0:
            aggregated[key]["ttc_weighted_sec"] += float(row.avg_time_to_convert_sec) * cc
            aggregated[key]["ttc_weight"] += cc
            ttc_bucket_days[int(float(row.avg_time_to_convert_sec) / 86400.0)] += cc

        path_len_counts[len(steps)] += cj
        if _is_direct_unknown(steps[-1]):
            journeys_ending_direct += cj

        for step in steps:
            token = step.split(":", 1)[0]
            channel_frequency[token] += cj
            total_touchpoints += cj
            if _is_direct_unknown(token):
                direct_touchpoints += cj

    total_journeys = int(sum(v["count_journeys"] for v in aggregated.values()))
    if total_journeys <= 0:
        return {
            "total_journeys": 0,
            "avg_path_length": 0,
            "avg_time_to_conversion_days": None,
            "common_paths": [],
            "channel_frequency": {},
            "path_length_distribution": {"min": 0, "max": 0, "median": 0, "p90": 0},
            "time_to_conversion_distribution": None,
            "direct_unknown_diagnostics": {"touchpoint_share": 0.0, "journeys_ending_direct_share": 0.0},
            "config": None,
            "view_filters": {"direct_mode": direct_mode, "path_scope": "all" if mode == "all_journeys" else "converted"},
            "nba_config": nba_config or {},
            "next_best_by_prefix": {},
            "next_best_by_prefix_campaign": {},
            "source": "journey_paths_daily",
        }

    common_paths: List[Dict[str, Any]] = []
    weighted_len_sum = 0.0
    weighted_ttc_sum = 0.0
    weighted_ttc_n = 0.0
    next_step_stats: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: {"count": 0.0, "conversions": 0.0}))

    for steps, vals in aggregated.items():
        count = int(vals["count_journeys"])
        conv = int(vals["count_conversions"])
        path = " > ".join(steps)
        share = (count / total_journeys) if total_journeys > 0 else 0.0
        avg_days = None
        if vals["ttc_weight"] > 0:
            avg_days = (vals["ttc_weighted_sec"] / vals["ttc_weight"]) / 86400.0
            weighted_ttc_sum += vals["ttc_weighted_sec"] / 86400.0
            weighted_ttc_n += vals["ttc_weight"]
        weighted_len_sum += len(steps) * count

        common_paths.append(
            {
                "path": path,
                "count": count,
                "share": round(share, 6),
                "avg_time_to_convert_days": round(avg_days, 4) if avg_days is not None else None,
                "path_length": len(steps),
            }
        )

        for idx in range(1, len(steps)):
            prefix = " > ".join(steps[:idx])
            nxt = steps[idx]
            next_step_stats[prefix][nxt]["count"] += count
            next_step_stats[prefix][nxt]["conversions"] += conv

    common_paths.sort(key=lambda p: p["count"], reverse=True)

    next_best_by_prefix: Dict[str, List[Dict[str, Any]]] = {}
    for prefix, recs in next_step_stats.items():
        rec_list: List[Dict[str, Any]] = []
        for step, stats in recs.items():
            cnt = float(stats["count"])
            conv = float(stats["conversions"])
            rate = (conv / cnt) if cnt > 0 else 0.0
            rec_list.append(
                {
                    "channel": step,
                    "campaign": None,
                    "step": step,
                    "count": int(cnt),
                    "conversions": int(conv),
                    "conversion_rate": round(rate, 6),
                    "avg_value": 0.0,
                }
            )
        rec_list.sort(key=lambda r: (r["conversion_rate"], r["count"]), reverse=True)
        next_best_by_prefix[prefix] = rec_list[:5]

    path_len_min = min(path_len_counts) if path_len_counts else 0
    path_len_max = max(path_len_counts) if path_len_counts else 0
    avg_path_length = round((weighted_len_sum / total_journeys), 3) if total_journeys > 0 else 0.0

    time_dist = None
    if ttc_bucket_days:
        tmin = min(ttc_bucket_days)
        tmax = max(ttc_bucket_days)
        tmed = _weighted_quantile(ttc_bucket_days, 0.5)
        tp90 = _weighted_quantile(ttc_bucket_days, 0.9)
        time_dist = {"min": tmin, "max": tmax, "median": tmed, "p90": tp90}

    return {
        "total_journeys": total_journeys,
        "avg_path_length": avg_path_length,
        "avg_time_to_conversion_days": round(weighted_ttc_sum / weighted_ttc_n, 4) if weighted_ttc_n > 0 else None,
        "common_paths": common_paths[:50],
        "channel_frequency": dict(sorted(channel_frequency.items(), key=lambda kv: kv[1], reverse=True)),
        "path_length_distribution": {
            "min": path_len_min,
            "max": path_len_max,
            "median": _weighted_quantile(path_len_counts, 0.5),
            "p90": _weighted_quantile(path_len_counts, 0.9),
        },
        "time_to_conversion_distribution": time_dist,
        "direct_unknown_diagnostics": {
            "touchpoint_share": (direct_touchpoints / total_touchpoints) if total_touchpoints > 0 else 0.0,
            "journeys_ending_direct_share": (journeys_ending_direct / total_journeys) if total_journeys > 0 else 0.0,
        },
        "config": None,
        "view_filters": {
            "direct_mode": direct_mode,
            "path_scope": "all" if mode == "all_journeys" else "converted",
        },
        "nba_config": nba_config or {},
        "next_best_by_prefix": next_best_by_prefix,
        "next_best_by_prefix_campaign": {},
        "source": "journey_paths_daily",
        "journey_definition_id": definition.id,
        "date_from": start_d.isoformat(),
        "date_to": end_d.isoformat(),
    }


def build_conversion_path_details_from_daily(
    db: Session,
    *,
    path: str,
    definition_id: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    direct_mode: str = "include",
    path_scope: str = "converted",
) -> Dict[str, Any]:
    analysis = build_conversion_paths_analysis_from_daily(
        db,
        definition_id=definition_id,
        date_from=date_from,
        date_to=date_to,
        direct_mode=direct_mode,
        path_scope=path_scope,
        nba_config=None,
    )
    rows = analysis.get("common_paths") or []
    target = next((r for r in rows if r.get("path") == path), None)
    if not target:
        raise ValueError("Path not found for selected filters")

    total = float(analysis.get("total_journeys") or 0)
    steps = [s for s in str(path).split(" > ") if s]

    # Prefix-based step drop-off estimate from aggregate rows
    by_prefix_count: Dict[str, float] = defaultdict(float)
    by_full_count: Dict[str, float] = defaultdict(float)
    for r in rows:
        p = str(r.get("path") or "")
        count = float(r.get("count") or 0)
        p_steps = [s for s in p.split(" > ") if s]
        by_full_count[p] += count
        for idx in range(1, len(p_steps) + 1):
            pref = " > ".join(p_steps[:idx])
            by_prefix_count[pref] += count

    step_breakdown = []
    for idx in range(1, len(steps) + 1):
        pref = " > ".join(steps[:idx])
        pref_cnt = by_prefix_count.get(pref, 0.0)
        stops_here = sum(
            c for p, c in by_full_count.items() if p == pref
        )
        dropoff_share = (stops_here / pref_cnt) if pref_cnt > 0 else 0.0
        step_breakdown.append(
            {
                "step": steps[idx - 1],
                "position": idx,
                "dropoff_share": round(dropoff_share, 6),
                "prefix_journeys": int(pref_cnt),
            }
        )

    return {
        "path": path,
        "summary": {
            "count": int(target.get("count") or 0),
            "share": float(target.get("share") or 0.0),
            "avg_touchpoints": float(target.get("path_length") or len(steps)),
            "avg_time_to_convert_days": target.get("avg_time_to_convert_days"),
        },
        "step_breakdown": step_breakdown,
        "variants": [
            {
                "path": target.get("path"),
                "count": int(target.get("count") or 0),
                "share": float(target.get("share") or 0.0),
            }
        ],
        "data_health": {
            "direct_unknown_touch_share": float((analysis.get("direct_unknown_diagnostics") or {}).get("touchpoint_share") or 0.0),
            "journeys_ending_direct_share": float((analysis.get("direct_unknown_diagnostics") or {}).get("journeys_ending_direct_share") or 0.0),
            "confidence": None,
        },
    }
