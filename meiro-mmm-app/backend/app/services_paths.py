"""Helpers for path archetypes and simple anomaly detection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class PathRecord:
    path: str
    count: int
    avg_length: float
    avg_time_to_conv_days: Optional[float]
    top_channels: List[str]


def _extract_paths(journeys: List[Dict[str, Any]]) -> List[PathRecord]:
    """Extract simple path statistics from journeys (converted only)."""
    if not journeys:
        return []

    rows: List[Dict[str, Any]] = []
    for j in journeys:
        if not j.get("converted", True):
            continue
        tps = j.get("touchpoints", [])
        if not tps:
            continue
        channels = [tp.get("channel", "unknown") for tp in tps]
        path_str = " > ".join(channels)
        first_ts = tps[0].get("timestamp")
        last_ts = tps[-1].get("timestamp")
        try:
            t_first = pd.to_datetime(first_ts) if first_ts else None
            t_last = pd.to_datetime(last_ts) if last_ts else None
        except Exception:
            t_first = None
            t_last = None
        delta_days: Optional[float] = None
        if t_first is not None and t_last is not None and t_last >= t_first:
            delta_days = float((t_last - t_first).total_seconds() / 86400.0)
        rows.append(
            {
                "path": path_str,
                "length": len(tps),
                "time_to_conv": delta_days,
                "channels": channels,
            }
        )

    if not rows:
        return []

    df = pd.DataFrame(rows)
    grouped = df.groupby("path")
    out: List[PathRecord] = []
    for path, g in grouped:
        count = int(len(g))
        avg_len = float(g["length"].mean())
        if g["time_to_conv"].notna().any():
            avg_time = float(g["time_to_conv"].dropna().mean())
        else:
            avg_time = None
        # top channels by frequency across all occurrences of this path
        all_channels: List[str] = []
        for ch_list in g["channels"]:
            all_channels.extend(ch_list)
        if all_channels:
            ch_series = pd.Series(all_channels)
            top_channels = list(ch_series.value_counts().head(3).index.astype(str))
        else:
            top_channels = []
        out.append(
            PathRecord(
                path=path,
                count=count,
                avg_length=avg_len,
                avg_time_to_conv_days=avg_time,
                top_channels=top_channels,
            )
        )
    # Sort by count desc
    out.sort(key=lambda r: r.count, reverse=True)
    return out


def compute_path_archetypes(journeys: List[Dict[str, Any]], conversion_key: Optional[str] = None) -> Dict[str, Any]:
    """Compute simple path archetypes from journeys.

    For now, each distinct path is its own archetype; we take the top N by volume.
    """
    recs = _extract_paths(journeys)
    if not recs:
        return {"clusters": [], "total_converted": 0}

    total_converted = sum(r.count for r in recs)
    top_n = 10
    clusters = []
    for idx, r in enumerate(recs[:top_n]):
        clusters.append(
            {
                "id": idx + 1,
                "name": r.path,
                "size": r.count,
                "share": r.count / total_converted if total_converted else 0.0,
                "avg_length": r.avg_length,
                "avg_time_to_conversion_days": r.avg_time_to_conv_days,
                "top_channels": r.top_channels,
                "top_paths": [
                    {
                        "path": r.path,
                        "count": r.count,
                        "share": r.count / total_converted if total_converted else 0.0,
                    }
                ],
            }
        )
    return {"clusters": clusters, "total_converted": total_converted}


def compute_path_anomalies(journeys: List[Dict[str, Any]], conversion_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """Very lightweight anomaly hints based on current journeys only.

    This is intentionally simple and non-persistent. It highlights:
      - High share of 'unknown' channels
      - Very long maximum path length
    """
    if not journeys:
        return []
    recs = _extract_paths(journeys)
    if not recs:
        return []

    anomalies: List[Dict[str, Any]] = []

    # Unknown channel share
    unknown_touchpoints = 0
    total_touchpoints = 0
    for j in journeys:
        for tp in j.get("touchpoints", []):
            total_touchpoints += 1
            if tp.get("channel") == "unknown":
                unknown_touchpoints += 1
    if total_touchpoints > 0:
        share_unknown = unknown_touchpoints / total_touchpoints
        if share_unknown > 0.3:
            anomalies.append(
                {
                    "type": "high_unknown_share",
                    "severity": "warn",
                    "metric_key": "unknown_channel_share",
                    "metric_value": share_unknown,
                    "message": f"{share_unknown:.1%} of touchpoints use 'unknown' channel. Check taxonomy and tracking.",
                }
            )

    # Long paths
    max_len = max(r.avg_length for r in recs) if recs else 0.0
    if max_len > 8:
        anomalies.append(
            {
                "type": "long_paths",
                "severity": "info",
                "metric_key": "max_path_length",
                "metric_value": max_len,
                "message": f"Some paths are very long (up to {max_len:.1f} steps). Consider capping analysis window.",
            }
        )

    return anomalies
