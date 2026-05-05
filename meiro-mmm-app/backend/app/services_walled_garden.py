from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


WALLED_GARDEN_CHANNELS = {
    "meta_ads",
    "facebook_ads",
    "instagram_ads",
    "youtube_ads",
    "google_ads",
    "tiktok_ads",
    "linkedin_ads",
}

CHANNEL_ALIASES = {
    "meta": "meta_ads",
    "facebook": "meta_ads",
    "facebook_ads": "meta_ads",
    "instagram": "meta_ads",
    "instagram_ads": "meta_ads",
    "youtube": "youtube_ads",
    "youtube_ads": "youtube_ads",
    "google": "google_ads",
    "google_ads": "google_ads",
    "tiktok": "tiktok_ads",
    "tiktok_ads": "tiktok_ads",
    "linkedin": "linkedin_ads",
    "linkedin_ads": "linkedin_ads",
}

QUALITY_DEFAULTS = {
    "meta_ads": 0.72,
    "youtube_ads": 0.78,
    "google_ads": 0.58,
    "tiktok_ads": 0.68,
    "linkedin_ads": 0.70,
}


def normalize_channel(channel: Any) -> str:
    raw = str(channel or "").strip().lower().replace(" ", "_")
    return CHANNEL_ALIASES.get(raw, raw)


def synthetic_column(channel: str) -> str:
    return f"{normalize_channel(channel)}_synthetic_impressions"


def source_channel_from_synthetic_column(column: str) -> str:
    suffix = "_synthetic_impressions"
    if column.endswith(suffix):
        return normalize_channel(column[: -len(suffix)])
    return normalize_channel(column)


def is_walled_garden(channel: str) -> bool:
    return normalize_channel(channel) in WALLED_GARDEN_CHANNELS


def _numeric(row: Any, key: str) -> float:
    try:
        return float(row.get(key, 0) or 0)
    except Exception:
        return 0.0


def _quality_weight(channel: str) -> float:
    return QUALITY_DEFAULTS.get(normalize_channel(channel), 0.60)


def calculate_synthetic_impressions(row: Any) -> Tuple[float, Dict[str, Any]]:
    channel = normalize_channel(row.get("channel"))
    impressions = max(_numeric(row, "impressions"), 0.0)
    reach = max(_numeric(row, "reach"), 0.0)
    frequency = max(_numeric(row, "frequency"), 0.0)
    video_views = max(_numeric(row, "video_views"), _numeric(row, "views"), 0.0)
    completed_views = max(_numeric(row, "completed_views"), _numeric(row, "video_completions"), 0.0)
    clicks = max(_numeric(row, "clicks"), 0.0)

    if frequency <= 0 and reach > 0 and impressions > 0:
        frequency = impressions / max(reach, 1.0)

    quality = _quality_weight(channel)
    frequency_damping = 1.0
    if frequency > 3:
        frequency_damping = 1.0 / (1.0 + 0.08 * (frequency - 3.0))

    base_pressure = impressions * quality * frequency_damping
    engagement_pressure = video_views * 0.35 + completed_views * 0.70 + clicks * 4.0
    synthetic = max(base_pressure + engagement_pressure, 0.0)

    components = {
        "quality_weight": quality,
        "frequency": frequency if frequency > 0 else None,
        "frequency_damping": frequency_damping,
        "base_pressure": base_pressure,
        "engagement_pressure": engagement_pressure,
    }
    return synthetic, components


def load_ads_delivery_rows(data_dir: Path, *, date_start: str, date_end: str) -> List[Dict[str, Any]]:
    sources = [
        data_dir / "unified_ads.csv",
        data_dir / "meta_ads.csv",
        data_dir / "google_ads.csv",
        data_dir / "linkedin_ads.csv",
        data_dir / "tiktok_ads.csv",
        data_dir / "youtube_ads.csv",
    ]
    frames: List[pd.DataFrame] = []
    for path in sources:
        if not path.exists():
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if frame.empty or "channel" not in frame.columns:
            continue
        frame["__source_file"] = path.name
        frames.append(frame)
        if path.name == "unified_ads.csv":
            break
    if not frames:
        return []

    df = pd.concat(frames, ignore_index=True)
    if "date" not in df.columns:
        return []
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    start = pd.to_datetime(date_start, errors="coerce")
    end = pd.to_datetime(date_end, errors="coerce")
    if pd.notna(start):
        df = df[df["date"] >= start]
    if pd.notna(end):
        df = df[df["date"] <= end]
    if df.empty:
        return []

    df["channel"] = df["channel"].map(normalize_channel)
    numeric_cols = [
        "spend",
        "impressions",
        "reach",
        "frequency",
        "clicks",
        "video_views",
        "views",
        "completed_views",
        "video_completions",
        "conversions",
        "revenue",
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    if "campaign" not in df.columns:
        df["campaign"] = "unknown"

    rows = df.to_dict(orient="records")
    for row in rows:
        synthetic, components = calculate_synthetic_impressions(row)
        row["synthetic_impressions"] = synthetic
        row["synthetic_components"] = components
        row["date"] = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
    return rows


def aggregate_synthetic_by_week_channel(
    delivery_rows: Iterable[Dict[str, Any]],
    *,
    channels: List[str],
    week_start_fn: Any,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    selected = {normalize_channel(ch) for ch in channels}
    week_channel: Dict[Tuple[pd.Timestamp, str], Dict[str, float]] = {}
    details: Dict[str, Dict[str, Any]] = {}

    for row in delivery_rows:
        channel = normalize_channel(row.get("channel"))
        if selected and channel not in selected:
            continue
        ts = pd.to_datetime(row.get("date"), errors="coerce")
        if pd.isna(ts):
            continue
        week = week_start_fn(ts)
        if pd.isna(week) or not (date_start <= week <= date_end):
            continue
        synthetic = max(_numeric(row, "synthetic_impressions"), 0.0)
        if synthetic <= 0 and _numeric(row, "impressions") > 0:
            synthetic, _ = calculate_synthetic_impressions(row)
        key = (week, channel)
        bucket = week_channel.setdefault(
            key,
            {"synthetic_impressions": 0.0, "impressions": 0.0, "reach": 0.0, "clicks": 0.0, "spend": 0.0, "rows": 0.0},
        )
        bucket["synthetic_impressions"] += synthetic
        bucket["impressions"] += _numeric(row, "impressions")
        bucket["reach"] += _numeric(row, "reach")
        bucket["clicks"] += _numeric(row, "clicks")
        bucket["spend"] += _numeric(row, "spend")
        bucket["rows"] += 1.0
        detail = details.setdefault(
            channel,
            {
                "channel": channel,
                "synthetic_column": synthetic_column(channel),
                "rows": 0,
                "impressions": 0.0,
                "synthetic_impressions": 0.0,
                "reach": 0.0,
                "clicks": 0.0,
                "spend": 0.0,
                "has_reach": False,
                "has_frequency": False,
                "is_walled_garden": is_walled_garden(channel),
                "method": "impressions_quality_frequency_engagement_v1",
            },
        )
        detail["rows"] += 1
        detail["impressions"] += _numeric(row, "impressions")
        detail["synthetic_impressions"] += synthetic
        detail["reach"] += _numeric(row, "reach")
        detail["clicks"] += _numeric(row, "clicks")
        detail["spend"] += _numeric(row, "spend")
        detail["has_reach"] = bool(detail["has_reach"] or _numeric(row, "reach") > 0)
        detail["has_frequency"] = bool(detail["has_frequency"] or _numeric(row, "frequency") > 0)

    rows: List[Dict[str, Any]] = []
    for (week, channel), metrics in week_channel.items():
        rows.append(
            {
                "date": week,
                "channel": channel,
                "synthetic_column": synthetic_column(channel),
                **metrics,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["date", "channel", "synthetic_column", "synthetic_impressions"])

    for detail in details.values():
        confidence_score = 0.25
        if detail["rows"] >= 4:
            confidence_score += 0.20
        if detail["impressions"] > 0:
            confidence_score += 0.20
        if detail["has_reach"] or detail["has_frequency"]:
            confidence_score += 0.20
        if detail["clicks"] > 0:
            confidence_score += 0.10
        if detail["spend"] > 0:
            confidence_score += 0.05
        detail["confidence_score"] = round(min(confidence_score, 1.0), 2)
        detail["confidence"] = "high" if confidence_score >= 0.75 else "medium" if confidence_score >= 0.50 else "low"
        caveats: List[str] = []
        if not detail["has_reach"] and not detail["has_frequency"]:
            caveats.append("No reach or frequency metric was imported; frequency damping uses defaults.")
        if detail["rows"] < 4:
            caveats.append("Limited delivery history; use as directional signal.")
        if detail["impressions"] <= 0:
            caveats.append("No platform impressions were imported for this period.")
        detail["caveats"] = caveats

    return df, {"channels": details, "method": "synthetic_impressions_v1"}
