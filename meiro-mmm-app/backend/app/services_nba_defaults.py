from __future__ import annotations

from typing import Any, Dict, List, Tuple

from app.attribution_engine import compute_next_best_action, has_any_campaign
from app.modules.settings.schemas import NBASettings
from app.services_settings_decisions import build_nba_preview_decision


def filter_nba_recommendations(
    nba_raw: Dict[str, List[Dict[str, Any]]],
    settings: NBASettings,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Apply NBA settings thresholds to raw recommendations and collect stats."""

    stats: Dict[str, Any] = {
        "total_before": 0,
        "total_after": 0,
        "filtered_support": 0,
        "filtered_conversion": 0,
        "filtered_uplift": 0,
        "filtered_excluded": 0,
        "filtered_depth": 0,
        "trimmed_cap": 0,
        "prefixes_considered": 0,
        "prefixes_retained": 0,
    }

    min_prefix_support = max(1, settings.min_prefix_support)
    min_conversion_rate = max(0.0, settings.min_conversion_rate)
    max_prefix_depth = max(0, settings.max_prefix_depth)
    min_next_support = max(1, settings.min_next_support or settings.min_prefix_support)
    max_suggestions = max(1, settings.max_suggestions_per_prefix)
    min_uplift_pct = settings.min_uplift_pct
    excluded_channels = {
        ch.strip().lower() for ch in (settings.excluded_channels or []) if ch
    }

    filtered: Dict[str, List[Dict[str, Any]]] = {}

    for prefix, recs in nba_raw.items():
        stats["prefixes_considered"] += 1
        stats["total_before"] += len(recs)

        depth = len([step for step in prefix.split(" > ") if step])
        if depth > max_prefix_depth:
            stats["filtered_depth"] += len(recs)
            continue

        prefix_support = sum(int(r.get("count", 0)) for r in recs)
        if prefix_support < min_prefix_support:
            stats["filtered_support"] += len(recs)
            continue

        total_conversions = sum(int(r.get("conversions", 0)) for r in recs)
        baseline_rate = (
            total_conversions / prefix_support if prefix_support > 0 else 0.0
        )

        kept: List[Dict[str, Any]] = []
        for rec in recs:
            count = int(rec.get("count", 0))
            conv_rate = float(rec.get("conversion_rate", 0.0))
            channel = str(rec.get("channel", "")).lower()

            if count < min_next_support:
                stats["filtered_support"] += 1
                continue
            if conv_rate < min_conversion_rate:
                stats["filtered_conversion"] += 1
                continue
            if excluded_channels and channel in excluded_channels:
                stats["filtered_excluded"] += 1
                continue
            if (
                min_uplift_pct is not None
                and min_uplift_pct > 0
                and baseline_rate > 0
            ):
                uplift = (conv_rate - baseline_rate) / baseline_rate
                if uplift < min_uplift_pct:
                    stats["filtered_uplift"] += 1
                    continue

            kept.append(rec)

        if kept:
            if len(kept) > max_suggestions:
                stats["trimmed_cap"] += len(kept) - max_suggestions
                kept = kept[:max_suggestions]
            filtered[prefix] = kept
            stats["prefixes_retained"] += 1
            stats["total_after"] += len(kept)

    return filtered, stats


def build_nba_preview_summary(
    *,
    journeys: List[Dict[str, Any]],
    settings: NBASettings,
    level: str = "channel",
) -> Dict[str, Any]:
    if not journeys:
        reason = "Preview unavailable (no journeys loaded)"
        return {
            "previewAvailable": False,
            "datasetJourneys": 0,
            "totalPrefixes": 0,
            "prefixesEligible": 0,
            "totalRecommendations": 0,
            "averageRecommendationsPerPrefix": 0.0,
            "filteredBySupportPct": 0.0,
            "filteredByConversionPct": 0.0,
            "reason": reason,
            "decision": build_nba_preview_decision(
                preview_available=False,
                reason=reason,
                dataset_journeys=0,
                prefixes_eligible=0,
                total_prefixes=0,
                total_recommendations=0,
                filtered_by_support_pct=0.0,
                filtered_by_conversion_pct=0.0,
            ),
        }

    requested_level = (level or "channel").lower()
    use_level = (
        "campaign" if requested_level == "campaign" and has_any_campaign(journeys) else "channel"
    )
    reason = (
        "Campaign-level preview unavailable (journeys lack campaign data)"
        if requested_level == "campaign" and use_level != "campaign"
        else None
    )

    nba_raw = compute_next_best_action(journeys, level=use_level)
    _, stats = filter_nba_recommendations(nba_raw, settings)

    total_before = stats.get("total_before", 0) or 0
    support_filtered = (
        stats.get("filtered_support", 0) + stats.get("filtered_depth", 0)
    )
    conversion_filtered = stats.get("filtered_conversion", 0)
    prefixes_eligible = stats.get("prefixes_retained", 0)
    total_after = stats.get("total_after", 0)
    avg_per_prefix = (
        total_after / prefixes_eligible if prefixes_eligible else 0.0
    )

    filtered_support_pct = (
        (support_filtered / total_before) * 100 if total_before else 0.0
    )
    filtered_conversion_pct = (
        (conversion_filtered / total_before) * 100 if total_before else 0.0
    )

    rounded_support = round(filtered_support_pct, 2)
    rounded_conversion = round(filtered_conversion_pct, 2)

    return {
        "previewAvailable": True,
        "datasetJourneys": len(journeys),
        "totalPrefixes": stats.get("prefixes_considered", 0),
        "prefixesEligible": prefixes_eligible,
        "totalRecommendations": total_after,
        "averageRecommendationsPerPrefix": round(avg_per_prefix, 2),
        "filteredBySupportPct": rounded_support,
        "filteredByConversionPct": rounded_conversion,
        "reason": reason,
        "decision": build_nba_preview_decision(
            preview_available=True,
            reason=reason,
            dataset_journeys=len(journeys),
            prefixes_eligible=prefixes_eligible,
            total_prefixes=stats.get("prefixes_considered", 0),
            total_recommendations=total_after,
            filtered_by_support_pct=rounded_support,
            filtered_by_conversion_pct=rounded_conversion,
        ),
    }
