from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.services_canonical_facts import count_canonical_conversions
from app.services_taxonomy import (
    compute_taxonomy_coverage,
    compute_taxonomy_coverage_from_db,
    compute_touchpoint_confidence,
    compute_unknown_share,
    compute_unknown_share_from_db,
    map_to_channel,
    _normalized_text,
)
from app.models_config_dq import ConversionTaxonomyTouchpointFact
from app.utils.taxonomy import Taxonomy, load_taxonomy


def _confidence_band(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.5:
        return "medium"
    return "low"


def _build_attention_queue(
    *,
    unknown_patterns: List[Dict[str, Any]],
    low_confidence_patterns: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    queue: List[Dict[str, Any]] = []
    for item in unknown_patterns[:5]:
        queue.append(
            {
                "type": "unmapped_pattern",
                "title": f"{item.get('source') or '—'} / {item.get('medium') or '—'}",
                "detail": f"{int(item.get('count') or 0)} touchpoints currently unresolved",
                "count": int(item.get("count") or 0),
                "sample": {
                    "source": item.get("source") or "",
                    "medium": item.get("medium") or "",
                    "campaign": item.get("campaign"),
                },
            }
        )
    for item in low_confidence_patterns[:5]:
        queue.append(
            {
                "type": "low_confidence_pattern",
                "title": f"{item.get('source') or '—'} / {item.get('medium') or '—'}",
                "detail": f"Confidence {float(item.get('confidence') or 0):.0%} across {int(item.get('count') or 0)} touchpoints",
                "count": int(item.get("count") or 0),
                "sample": {
                    "source": item.get("source") or "",
                    "medium": item.get("medium") or "",
                    "campaign": item.get("campaign"),
                },
            }
        )
    queue.sort(key=lambda item: -int(item.get("count") or 0))
    return queue[:8]


def _compute_low_confidence_patterns(
    journeys: List[Dict[str, Any]],
    taxonomy: Taxonomy,
    *,
    limit: int = 8,
) -> Dict[str, Any]:
    total_touchpoints = 0
    low_confidence_count = 0
    pattern_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    pattern_confidences: dict[tuple[str, str, str], list[float]] = defaultdict(list)

    for journey in journeys:
        for tp in journey.get("touchpoints", []):
            total_touchpoints += 1
            utm = tp.get("utm") or {}
            source = _normalized_text(tp.get("utm_source") or (utm.get("source") if isinstance(utm, dict) else None) or tp.get("source"))
            medium = _normalized_text(tp.get("utm_medium") or (utm.get("medium") if isinstance(utm, dict) else None) or tp.get("medium"))
            campaign = _normalized_text(tp.get("utm_campaign") or (utm.get("campaign") if isinstance(utm, dict) else None) or tp.get("campaign"))
            confidence = compute_touchpoint_confidence(tp, taxonomy)
            if confidence < 0.5:
                low_confidence_count += 1
                key = (source, medium, campaign)
                pattern_counts[key] += 1
                pattern_confidences[key].append(confidence)

    top_patterns = sorted(pattern_counts.items(), key=lambda item: -item[1])[:limit]
    return {
        "count": low_confidence_count,
        "share": (low_confidence_count / total_touchpoints) if total_touchpoints else 0.0,
        "top_patterns": [
            {
                "source": source,
                "medium": medium,
                "campaign": campaign or None,
                "count": count,
                "confidence": (
                    sum(pattern_confidences[(source, medium, campaign)])
                    / len(pattern_confidences[(source, medium, campaign)])
                )
                if pattern_confidences[(source, medium, campaign)]
                else 0.0,
            }
            for (source, medium, campaign), count in top_patterns
        ],
    }


def _compute_low_confidence_patterns_from_db(
    db: Session,
    taxonomy: Taxonomy,
    *,
    limit: int = 8,
    sample_limit: int = 50000,
) -> Optional[Dict[str, Any]]:
    rows = (
        db.query(ConversionTaxonomyTouchpointFact)
        .order_by(ConversionTaxonomyTouchpointFact.conversion_ts.desc(), ConversionTaxonomyTouchpointFact.ordinal.asc())
        .limit(sample_limit)
        .all()
    )
    if not rows:
        return None
    raw_count = count_canonical_conversions(db, limit=sample_limit)
    observed_conversion_ids = {str(row.conversion_id or "") for row in rows}
    if raw_count > len(observed_conversion_ids):
        return None

    total_touchpoints = 0
    low_confidence_count = 0
    pattern_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    pattern_confidences: dict[tuple[str, str, str], list[float]] = defaultdict(list)

    for row in rows:
        total_touchpoints += 1
        source = _normalized_text(row.source)
        medium = _normalized_text(row.medium)
        campaign = _normalized_text(row.campaign)
        confidence = map_to_channel(source, medium, campaign, taxonomy).confidence
        if confidence < 0.5:
            low_confidence_count += 1
            key = (source, medium, campaign)
            pattern_counts[key] += 1
            pattern_confidences[key].append(confidence)

    top_patterns = sorted(pattern_counts.items(), key=lambda item: -item[1])[:limit]
    return {
        "count": low_confidence_count,
        "share": (low_confidence_count / total_touchpoints) if total_touchpoints else 0.0,
        "top_patterns": [
            {
                "source": source,
                "medium": medium,
                "campaign": campaign or None,
                "count": count,
                "confidence": (
                    sum(pattern_confidences[(source, medium, campaign)])
                    / len(pattern_confidences[(source, medium, campaign)])
                )
                if pattern_confidences[(source, medium, campaign)]
                else 0.0,
            }
            for (source, medium, campaign), count in top_patterns
        ],
    }


def _recommended_actions(
    *,
    unknown_share: float,
    low_confidence_share: float,
    suggestion_count: int,
) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    if suggestion_count > 0:
        actions.append(
            {
                "id": "review_suggestions",
                "label": f"Review {suggestion_count} taxonomy suggestions",
                "benefit": "Reduce unknown traffic with low-risk draft changes",
                "requires_review": True,
                "domain": "taxonomy",
                "target_page": "settings",
                "target_section": "taxonomy",
                "target_tab": "suggestions",
            }
        )
    if unknown_share >= 0.1:
        actions.append(
            {
                "id": "reduce_unknown_share",
                "label": "Reduce unknown traffic before relying on attribution outputs",
                "benefit": "Improve channel-level reporting consistency",
                "requires_review": True,
                "domain": "taxonomy",
                "target_page": "settings",
                "target_section": "taxonomy",
                "target_tab": "suggestions",
            }
        )
    if low_confidence_share >= 0.1:
        actions.append(
            {
                "id": "review_low_confidence",
                "label": "Review low-confidence channel mappings",
                "benefit": "Raise trust in summaries and suggestions",
                "requires_review": True,
                "domain": "taxonomy",
                "target_page": "settings",
                "target_section": "taxonomy",
                "target_tab": "advanced",
            }
        )
    return actions


def _rule_overlap_warnings(taxonomy: Taxonomy) -> List[str]:
    warnings: List[str] = []
    enabled_rules = [rule for rule in taxonomy.channel_rules if rule.enabled]
    for index, rule in enumerate(enabled_rules):
        for other in enabled_rules[index + 1 :]:
            source_overlap = (
                rule.source.normalize_operator() == other.source.normalize_operator()
                and (rule.source.value or "").strip().lower() == (other.source.value or "").strip().lower()
            )
            medium_overlap = (
                rule.medium.normalize_operator() == other.medium.normalize_operator()
                and (rule.medium.value or "").strip().lower() == (other.medium.value or "").strip().lower()
            )
            campaign_overlap = (
                rule.campaign.normalize_operator() == other.campaign.normalize_operator()
                and (rule.campaign.value or "").strip().lower() == (other.campaign.value or "").strip().lower()
            )
            if source_overlap and medium_overlap and campaign_overlap:
                warnings.append(f"Rules '{rule.name or rule.channel}' and '{other.name or other.channel}' have identical match conditions.")
    return warnings[:5]


def build_taxonomy_overview(
    journeys: List[Dict[str, Any]],
    *,
    taxonomy: Optional[Taxonomy] = None,
    suggestion_count: int = 0,
) -> Dict[str, Any]:
    taxonomy = taxonomy or load_taxonomy()
    unknown_report = compute_unknown_share(journeys, taxonomy=taxonomy, sample_size=10)
    coverage = compute_taxonomy_coverage(journeys, taxonomy=taxonomy)
    low_confidence = _compute_low_confidence_patterns(journeys, taxonomy)
    status = "ready"
    if unknown_report.unknown_share >= 0.2:
        status = "blocked"
    elif unknown_report.unknown_share >= 0.08 or low_confidence["share"] >= 0.1:
        status = "warning"

    confidence_score = max(
        0.0,
        min(1.0, 1.0 - unknown_report.unknown_share * 0.65 - low_confidence["share"] * 0.35),
    )

    return {
        "status": status,
        "confidence": {
            "score": round(confidence_score, 3),
            "band": _confidence_band(confidence_score),
        },
        "summary": {
            "unknown_share": unknown_report.unknown_share,
            "unknown_count": unknown_report.unknown_count,
            "total_touchpoints": unknown_report.total_touchpoints,
            "source_coverage": coverage.get("source_coverage", 0.0),
            "medium_coverage": coverage.get("medium_coverage", 0.0),
            "active_rules": len([rule for rule in taxonomy.channel_rules if rule.enabled]),
            "source_aliases": len(taxonomy.source_aliases),
            "medium_aliases": len(taxonomy.medium_aliases),
            "low_confidence_share": low_confidence["share"],
            "low_confidence_count": low_confidence["count"],
        },
        "top_unmapped_patterns": coverage.get("top_unmapped_patterns", [])[:8],
        "top_low_confidence_patterns": low_confidence["top_patterns"],
        "attention_queue": _build_attention_queue(
            unknown_patterns=coverage.get("top_unmapped_patterns", []),
            low_confidence_patterns=low_confidence["top_patterns"],
        ),
        "warnings": _rule_overlap_warnings(taxonomy),
        "recommended_actions": _recommended_actions(
            unknown_share=unknown_report.unknown_share,
            low_confidence_share=low_confidence["share"],
            suggestion_count=suggestion_count,
        ),
    }


def build_taxonomy_overview_from_db(
    db: Session,
    *,
    taxonomy: Optional[Taxonomy] = None,
    suggestion_count: int = 0,
) -> Optional[Dict[str, Any]]:
    taxonomy = taxonomy or load_taxonomy()
    unknown_report = compute_unknown_share_from_db(db, taxonomy=taxonomy, sample_size=10)
    coverage = compute_taxonomy_coverage_from_db(db, taxonomy=taxonomy)
    low_confidence = _compute_low_confidence_patterns_from_db(db, taxonomy)
    if unknown_report is None or coverage is None or low_confidence is None:
        return None

    status = "ready"
    if unknown_report.unknown_share >= 0.2:
        status = "blocked"
    elif unknown_report.unknown_share >= 0.08 or low_confidence["share"] >= 0.1:
        status = "warning"

    confidence_score = max(
        0.0,
        min(1.0, 1.0 - unknown_report.unknown_share * 0.65 - low_confidence["share"] * 0.35),
    )

    return {
        "status": status,
        "confidence": {
            "score": round(confidence_score, 3),
            "band": _confidence_band(confidence_score),
        },
        "summary": {
            "unknown_share": unknown_report.unknown_share,
            "unknown_count": unknown_report.unknown_count,
            "total_touchpoints": unknown_report.total_touchpoints,
            "source_coverage": coverage.get("source_coverage", 0.0),
            "medium_coverage": coverage.get("medium_coverage", 0.0),
            "active_rules": len([rule for rule in taxonomy.channel_rules if rule.enabled]),
            "source_aliases": len(taxonomy.source_aliases),
            "medium_aliases": len(taxonomy.medium_aliases),
            "low_confidence_share": low_confidence["share"],
            "low_confidence_count": low_confidence["count"],
        },
        "top_unmapped_patterns": coverage.get("top_unmapped_patterns", [])[:8],
        "top_low_confidence_patterns": low_confidence["top_patterns"],
        "attention_queue": _build_attention_queue(
            unknown_patterns=coverage.get("top_unmapped_patterns", []),
            low_confidence_patterns=low_confidence["top_patterns"],
        ),
        "warnings": _rule_overlap_warnings(taxonomy),
        "recommended_actions": _recommended_actions(
            unknown_share=unknown_report.unknown_share,
            low_confidence_share=low_confidence["share"],
            suggestion_count=suggestion_count,
        ),
    }


def _resolved_patterns(
    before_patterns: List[Dict[str, Any]],
    after_taxonomy: Taxonomy,
) -> List[Dict[str, Any]]:
    resolved: List[Dict[str, Any]] = []
    for item in before_patterns:
        source = item.get("source") or ""
        medium = item.get("medium") or ""
        campaign = item.get("campaign") or ""
        mapping = map_to_channel(source, medium, campaign, after_taxonomy)
        if mapping.channel != "unknown" and mapping.confidence >= 0.5:
            resolved.append(
                {
                    "source": source,
                    "medium": medium,
                    "campaign": campaign or None,
                    "count": int(item.get("count") or 0),
                    "channel": mapping.channel,
                    "confidence": mapping.confidence,
                }
            )
    resolved.sort(key=lambda item: -int(item["count"]))
    return resolved[:8]


def build_taxonomy_preview(
    journeys: List[Dict[str, Any]],
    *,
    current_taxonomy: Optional[Taxonomy] = None,
    draft_taxonomy: Optional[Taxonomy] = None,
) -> Dict[str, Any]:
    current_taxonomy = current_taxonomy or load_taxonomy()
    draft_taxonomy = draft_taxonomy or current_taxonomy

    before_overview = build_taxonomy_overview(journeys, taxonomy=current_taxonomy, suggestion_count=0)
    after_overview = build_taxonomy_overview(journeys, taxonomy=draft_taxonomy, suggestion_count=0)

    before_summary = before_overview["summary"]
    after_summary = after_overview["summary"]
    top_unmapped_before = before_overview.get("top_unmapped_patterns", [])

    return {
        "before": before_summary,
        "after": after_summary,
        "delta": {
            "unknown_share": round(float(after_summary["unknown_share"]) - float(before_summary["unknown_share"]), 6),
            "unknown_count": int(after_summary["unknown_count"]) - int(before_summary["unknown_count"]),
            "low_confidence_share": round(
                float(after_summary["low_confidence_share"]) - float(before_summary["low_confidence_share"]),
                6,
            ),
            "active_rules": int(after_summary["active_rules"]) - int(before_summary["active_rules"]),
            "source_aliases": int(after_summary["source_aliases"]) - int(before_summary["source_aliases"]),
            "medium_aliases": int(after_summary["medium_aliases"]) - int(before_summary["medium_aliases"]),
        },
        "top_new_matches": _resolved_patterns(top_unmapped_before, draft_taxonomy),
        "warnings": _rule_overlap_warnings(draft_taxonomy),
        "recommended_actions": after_overview.get("recommended_actions", []),
    }
