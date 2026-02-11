"""
Enhanced taxonomy service with UTM validation, channel mapping confidence,
unknown share tracking, and per-entity quality scoring.

Key features:
- Comprehensive UTM parameter validation and normalization
- Multi-level channel mapping with confidence scores
- Unknown/unmapped traffic tracking and alerts
- Per-touchpoint, per-journey, and per-channel confidence metrics
- Taxonomy coverage and mapping quality reports
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from .models_config_dq import DQSnapshot
from .utils.taxonomy import Taxonomy, ChannelRule, load_taxonomy, save_taxonomy

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# UTM validation
# ---------------------------------------------------------------------------


@dataclass
class UTMValidation:
    """Validation result for UTM parameters."""
    
    is_valid: bool
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    normalized: Dict[str, str] = field(default_factory=dict)
    confidence: float = 1.0  # 0.0 - 1.0


# Known UTM parameters
VALID_UTM_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", 
    "utm_content", "utm_id", "utm_source_platform"
}

# Common typos and variations
UTM_ALIASES = {
    "utmsource": "utm_source",
    "utm_src": "utm_source",
    "source": "utm_source",
    "utmmedium": "utm_medium",
    "utm_med": "utm_medium",
    "medium": "utm_medium",
    "utmcampaign": "utm_campaign",
    "utm_camp": "utm_campaign",
    "campaign": "utm_campaign",
    "utm_name": "utm_campaign",
}

# Reserved values that indicate missing/unknown
RESERVED_VALUES = {
    "(not set)", "(none)", "null", "undefined", "n/a", "na", "unknown", 
    "(not provided)", "(direct)", "not available", ""
}


def validate_utm_params(params: Dict[str, Any]) -> UTMValidation:
    """
    Validate and normalize UTM parameters.
    
    Returns validation result with warnings, errors, and confidence score.
    """
    warnings = []
    errors = []
    normalized = {}
    confidence = 1.0
    
    # Normalize parameter names
    for key, value in params.items():
        key_lower = key.lower().strip()
        
        # Check for known aliases
        if key_lower in UTM_ALIASES:
            canonical_key = UTM_ALIASES[key_lower]
            warnings.append(f"Non-standard parameter '{key}' mapped to '{canonical_key}'")
            normalized[canonical_key] = str(value).strip()
            confidence *= 0.95
        elif key_lower in VALID_UTM_PARAMS:
            normalized[key_lower] = str(value).strip()
        elif key_lower.startswith("utm_"):
            warnings.append(f"Unknown UTM parameter '{key}'")
            normalized[key_lower] = str(value).strip()
            confidence *= 0.9
    
    # Check for reserved values
    for key, value in normalized.items():
        value_lower = value.lower().strip()
        if value_lower in RESERVED_VALUES:
            warnings.append(f"Reserved value '{value}' in {key}")
            confidence *= 0.8
    
    # Validate required fields for attribution
    if "utm_source" not in normalized or not normalized["utm_source"]:
        errors.append("Missing utm_source (required for attribution)")
        confidence *= 0.5
    
    if "utm_medium" not in normalized or not normalized["utm_medium"]:
        warnings.append("Missing utm_medium (recommended for channel mapping)")
        confidence *= 0.9
    
    # Check for suspicious patterns
    for key, value in normalized.items():
        # Check for overly long values (likely errors)
        if len(value) > 200:
            warnings.append(f"{key} is unusually long ({len(value)} chars)")
            confidence *= 0.85
        
        # Check for special characters that might indicate encoding issues
        if re.search(r'[^\x00-\x7F]', value):
            warnings.append(f"{key} contains non-ASCII characters")
            confidence *= 0.95
    
    is_valid = len(errors) == 0
    
    return UTMValidation(
        is_valid=is_valid,
        warnings=warnings,
        errors=errors,
        normalized=normalized,
        confidence=max(0.0, min(1.0, confidence))
    )


# ---------------------------------------------------------------------------
# Channel mapping with confidence
# ---------------------------------------------------------------------------


@dataclass
class ChannelMapping:
    """Result of channel mapping operation."""
    
    channel: str
    matched_rule: Optional[str] = None
    confidence: float = 0.0  # 0.0 - 1.0
    source: Optional[str] = None
    medium: Optional[str] = None
    fallback_reason: Optional[str] = None


def map_to_channel(
    source: Optional[str],
    medium: Optional[str],
    taxonomy: Optional[Taxonomy] = None,
    default_channel: str = "unknown",
) -> ChannelMapping:
    """
    Map source/medium to channel with confidence score.
    
    Confidence scoring:
    - 1.0: Perfect match with both source and medium
    - 0.8: Match with source or medium only
    - 0.6: Fuzzy match
    - 0.3: Fallback to default
    - 0.0: No information
    """
    if taxonomy is None:
        taxonomy = load_taxonomy()
    
    source_clean = (source or "").lower().strip()
    medium_clean = (medium or "").lower().strip()
    
    # Case 1: No source and no medium
    if not source_clean and not medium_clean:
        return ChannelMapping(
            channel=default_channel,
            confidence=0.0,
            fallback_reason="No source or medium provided"
        )
    
    # Case 2: Reserved values
    if source_clean in RESERVED_VALUES and medium_clean in RESERVED_VALUES:
        return ChannelMapping(
            channel=default_channel,
            confidence=0.1,
            source=source_clean,
            medium=medium_clean,
            fallback_reason="Both source and medium are reserved values"
        )
    
    # Apply aliases
    source_normalized = taxonomy.source_aliases.get(source_clean, source_clean)
    medium_normalized = taxonomy.medium_aliases.get(medium_clean, medium_clean)
    
    # Try to match rules
    for rule in taxonomy.channel_rules:
        has_source_match = False
        has_medium_match = False
        
        if rule.source_regex:
            if re.search(rule.source_regex, source_normalized, re.IGNORECASE):
                has_source_match = True
        else:
            has_source_match = True  # No source requirement
        
        if rule.medium_regex:
            if re.search(rule.medium_regex, medium_normalized, re.IGNORECASE):
                has_medium_match = True
        else:
            has_medium_match = True  # No medium requirement
        
        if has_source_match and has_medium_match:
            # Perfect match
            if rule.source_regex and rule.medium_regex:
                confidence = 1.0
            elif rule.source_regex or rule.medium_regex:
                confidence = 0.8
            else:
                confidence = 0.6
            
            return ChannelMapping(
                channel=rule.channel,
                matched_rule=rule.name,
                confidence=confidence,
                source=source_normalized,
                medium=medium_normalized,
            )
    
    # No rule matched - fallback to default
    return ChannelMapping(
        channel=default_channel,
        confidence=0.3,
        source=source_normalized,
        medium=medium_normalized,
        fallback_reason="No matching taxonomy rule"
    )


def normalize_touchpoint_with_confidence(
    tp: Dict[str, Any],
    taxonomy: Optional[Taxonomy] = None,
) -> Tuple[Dict[str, Any], float]:
    """
    Normalize touchpoint with UTM validation and confidence scoring.
    
    Returns (normalized_touchpoint, confidence_score).
    """
    if taxonomy is None:
        taxonomy = load_taxonomy()
    
    # Extract UTM parameters
    utm_params = {
        k: v for k, v in tp.items()
        if k.lower().startswith("utm_") or k.lower() in {"source", "medium", "campaign"}
    }
    
    # Validate UTMs
    utm_validation = validate_utm_params(utm_params)
    confidence = utm_validation.confidence
    
    # Extract source/medium
    source = (
        utm_validation.normalized.get("utm_source") or
        tp.get("source") or
        ""
    )
    medium = (
        utm_validation.normalized.get("utm_medium") or
        tp.get("medium") or
        ""
    )
    
    # Map to channel
    channel_mapping = map_to_channel(source, medium, taxonomy)
    confidence *= channel_mapping.confidence
    
    # Build normalized touchpoint
    normalized = dict(tp)
    normalized["channel"] = channel_mapping.channel
    normalized["source"] = channel_mapping.source or source
    normalized["medium"] = channel_mapping.medium or medium
    normalized["campaign"] = (
        utm_validation.normalized.get("utm_campaign") or
        tp.get("campaign") or
        tp.get("utm_campaign")
    )
    normalized["utm_term"] = utm_validation.normalized.get("utm_term")
    normalized["utm_content"] = (
        utm_validation.normalized.get("utm_content") or
        tp.get("creative")
    )
    
    # Add metadata
    normalized["_confidence"] = confidence
    normalized["_utm_validation"] = {
        "is_valid": utm_validation.is_valid,
        "warnings": utm_validation.warnings,
        "errors": utm_validation.errors,
    }
    normalized["_channel_mapping"] = {
        "matched_rule": channel_mapping.matched_rule,
        "fallback_reason": channel_mapping.fallback_reason,
    }
    
    return normalized, confidence


# ---------------------------------------------------------------------------
# Unknown share tracking
# ---------------------------------------------------------------------------


@dataclass
class UnknownShareReport:
    """Report on unknown/unmapped traffic."""
    
    total_touchpoints: int
    unknown_count: int
    unknown_share: float
    by_source: Dict[str, int]
    by_medium: Dict[str, int]
    by_source_medium: Dict[Tuple[str, str], int]
    sample_unmapped: List[Dict[str, Any]]


def compute_unknown_share(
    journeys: List[Dict[str, Any]],
    taxonomy: Optional[Taxonomy] = None,
    sample_size: int = 20,
) -> UnknownShareReport:
    """
    Compute unknown/unmapped traffic share and identify patterns.
    
    Returns detailed report with unknown share, breakdowns, and samples.
    """
    if taxonomy is None:
        taxonomy = load_taxonomy()
    
    total_touchpoints = 0
    unknown_count = 0
    by_source = defaultdict(int)
    by_medium = defaultdict(int)
    by_source_medium = defaultdict(int)
    unmapped_samples = []
    
    for journey in journeys:
        touchpoints = journey.get("touchpoints", [])
        for tp in touchpoints:
            total_touchpoints += 1
            
            # Normalize and check channel
            source = (tp.get("utm_source") or tp.get("source") or "").lower().strip()
            medium = (tp.get("utm_medium") or tp.get("medium") or "").lower().strip()
            
            mapping = map_to_channel(source, medium, taxonomy)
            
            if mapping.channel == "unknown" or mapping.confidence < 0.5:
                unknown_count += 1
                by_source[source] += 1
                by_medium[medium] += 1
                by_source_medium[(source, medium)] += 1
                
                if len(unmapped_samples) < sample_size:
                    unmapped_samples.append({
                        "source": source,
                        "medium": medium,
                        "channel": mapping.channel,
                        "confidence": mapping.confidence,
                        "fallback_reason": mapping.fallback_reason,
                        "campaign": tp.get("campaign") or tp.get("utm_campaign"),
                    })
    
    unknown_share = unknown_count / total_touchpoints if total_touchpoints > 0 else 0.0
    
    return UnknownShareReport(
        total_touchpoints=total_touchpoints,
        unknown_count=unknown_count,
        unknown_share=unknown_share,
        by_source=dict(by_source),
        by_medium=dict(by_medium),
        by_source_medium=dict(by_source_medium),
        sample_unmapped=unmapped_samples,
    )


# ---------------------------------------------------------------------------
# Per-entity confidence scoring
# ---------------------------------------------------------------------------


def compute_touchpoint_confidence(tp: Dict[str, Any], taxonomy: Optional[Taxonomy] = None) -> float:
    """Compute confidence score for a single touchpoint."""
    _, confidence = normalize_touchpoint_with_confidence(tp, taxonomy)
    return confidence


def compute_journey_confidence(journey: Dict[str, Any], taxonomy: Optional[Taxonomy] = None) -> float:
    """
    Compute aggregate confidence score for a journey.
    
    Uses harmonic mean to penalize low-confidence touchpoints.
    """
    touchpoints = journey.get("touchpoints", [])
    if not touchpoints:
        return 0.0
    
    confidences = [compute_touchpoint_confidence(tp, taxonomy) for tp in touchpoints]
    
    # Harmonic mean (penalizes low scores more than arithmetic mean)
    if any(c == 0 for c in confidences):
        return 0.0
    
    harmonic_mean = len(confidences) / sum(1.0 / c for c in confidences)
    return harmonic_mean


def compute_channel_confidence(
    journeys: List[Dict[str, Any]],
    channel: str,
    taxonomy: Optional[Taxonomy] = None,
) -> Dict[str, Any]:
    """
    Compute confidence metrics for a specific channel across all journeys.
    
    Returns:
    - mean_confidence: Average confidence across all touchpoints in this channel
    - touchpoint_count: Number of touchpoints for this channel
    - low_confidence_count: Number of touchpoints with confidence < 0.5
    - sample_low_confidence: Sample of low-confidence touchpoints
    """
    if taxonomy is None:
        taxonomy = load_taxonomy()
    
    confidences = []
    low_confidence_samples = []
    
    for journey in journeys:
        touchpoints = journey.get("touchpoints", [])
        for tp in touchpoints:
            normalized, confidence = normalize_touchpoint_with_confidence(tp, taxonomy)
            
            if normalized.get("channel") == channel:
                confidences.append(confidence)
                
                if confidence < 0.5 and len(low_confidence_samples) < 10:
                    low_confidence_samples.append({
                        "source": normalized.get("source"),
                        "medium": normalized.get("medium"),
                        "campaign": normalized.get("campaign"),
                        "confidence": confidence,
                        "warnings": normalized.get("_utm_validation", {}).get("warnings", []),
                    })
    
    if not confidences:
        return {
            "mean_confidence": 0.0,
            "touchpoint_count": 0,
            "low_confidence_count": 0,
            "sample_low_confidence": [],
        }
    
    mean_confidence = sum(confidences) / len(confidences)
    low_confidence_count = sum(1 for c in confidences if c < 0.5)
    
    return {
        "mean_confidence": mean_confidence,
        "touchpoint_count": len(confidences),
        "low_confidence_count": low_confidence_count,
        "low_confidence_share": low_confidence_count / len(confidences),
        "sample_low_confidence": low_confidence_samples,
    }


# ---------------------------------------------------------------------------
# Taxonomy coverage and quality reports
# ---------------------------------------------------------------------------


def compute_taxonomy_coverage(
    journeys: List[Dict[str, Any]],
    taxonomy: Optional[Taxonomy] = None,
) -> Dict[str, Any]:
    """
    Compute taxonomy coverage metrics.
    
    Returns:
    - channel_distribution: Count of touchpoints per channel
    - source_coverage: % of sources with > 0.5 confidence mapping
    - medium_coverage: % of mediums with > 0.5 confidence mapping
    - rule_usage: Count of touchpoints matched by each rule
    - unmapped_patterns: Top unmapped source/medium combinations
    """
    if taxonomy is None:
        taxonomy = load_taxonomy()
    
    channel_dist = defaultdict(int)
    rule_usage = defaultdict(int)
    source_confidences = defaultdict(list)
    medium_confidences = defaultdict(list)
    unmapped_patterns = defaultdict(int)
    
    for journey in journeys:
        touchpoints = journey.get("touchpoints", [])
        for tp in touchpoints:
            source = (tp.get("utm_source") or tp.get("source") or "").lower().strip()
            medium = (tp.get("utm_medium") or tp.get("medium") or "").lower().strip()
            
            mapping = map_to_channel(source, medium, taxonomy)
            
            channel_dist[mapping.channel] += 1
            
            if mapping.matched_rule:
                rule_usage[mapping.matched_rule] += 1
            
            source_confidences[source].append(mapping.confidence)
            medium_confidences[medium].append(mapping.confidence)
            
            if mapping.confidence < 0.5:
                unmapped_patterns[(source, medium)] += 1
    
    # Compute coverage metrics
    sources_with_good_mapping = sum(
        1 for confidences in source_confidences.values()
        if sum(confidences) / len(confidences) >= 0.5
    )
    source_coverage = sources_with_good_mapping / len(source_confidences) if source_confidences else 0.0
    
    mediums_with_good_mapping = sum(
        1 for confidences in medium_confidences.values()
        if sum(confidences) / len(confidences) >= 0.5
    )
    medium_coverage = mediums_with_good_mapping / len(medium_confidences) if medium_confidences else 0.0
    
    # Top unmapped patterns
    top_unmapped = sorted(
        [(k, v) for k, v in unmapped_patterns.items()],
        key=lambda x: -x[1]
    )[:20]
    
    return {
        "channel_distribution": dict(channel_dist),
        "source_coverage": source_coverage,
        "medium_coverage": medium_coverage,
        "rule_usage": dict(rule_usage),
        "top_unmapped_patterns": [
            {"source": s, "medium": m, "count": count}
            for (s, m), count in top_unmapped
        ],
    }


# ---------------------------------------------------------------------------
# DQ snapshot integration
# ---------------------------------------------------------------------------


def persist_taxonomy_dq_snapshots(
    db: Session,
    journeys: List[Dict[str, Any]],
    taxonomy: Optional[Taxonomy] = None,
) -> List[DQSnapshot]:
    """
    Compute and persist taxonomy-related DQ snapshots.
    
    Metrics:
    - unknown_channel_share
    - mean_touchpoint_confidence
    - mean_journey_confidence
    - source_coverage
    - medium_coverage
    - low_confidence_touchpoint_share
    """
    if taxonomy is None:
        taxonomy = load_taxonomy()
    
    ts_bucket = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    
    # Unknown share
    unknown_report = compute_unknown_share(journeys, taxonomy)
    
    # Confidence scores
    touchpoint_confidences = []
    journey_confidences = []
    
    for journey in journeys:
        journey_conf = compute_journey_confidence(journey, taxonomy)
        journey_confidences.append(journey_conf)
        
        for tp in journey.get("touchpoints", []):
            tp_conf = compute_touchpoint_confidence(tp, taxonomy)
            touchpoint_confidences.append(tp_conf)
    
    mean_tp_conf = sum(touchpoint_confidences) / len(touchpoint_confidences) if touchpoint_confidences else 0.0
    mean_journey_conf = sum(journey_confidences) / len(journey_confidences) if journey_confidences else 0.0
    low_conf_share = sum(1 for c in touchpoint_confidences if c < 0.5) / len(touchpoint_confidences) if touchpoint_confidences else 0.0
    
    # Coverage
    coverage = compute_taxonomy_coverage(journeys, taxonomy)
    
    # Build snapshots
    snapshots = []
    
    metrics = [
        ("taxonomy", "unknown_channel_share", unknown_report.unknown_share, {
            "unknown_count": unknown_report.unknown_count,
            "total": unknown_report.total_touchpoints,
        }),
        ("taxonomy", "mean_touchpoint_confidence", mean_tp_conf, {
            "sample_size": len(touchpoint_confidences),
        }),
        ("taxonomy", "mean_journey_confidence", mean_journey_conf, {
            "sample_size": len(journey_confidences),
        }),
        ("taxonomy", "source_coverage", coverage["source_coverage"], {}),
        ("taxonomy", "medium_coverage", coverage["medium_coverage"], {}),
        ("taxonomy", "low_confidence_touchpoint_share", low_conf_share, {
            "threshold": 0.5,
        }),
    ]
    
    for source, metric_key, metric_value, meta in metrics:
        snap = DQSnapshot(
            ts_bucket=ts_bucket,
            source=source,
            metric_key=metric_key,
            metric_value=float(metric_value),
            meta_json=meta,
        )
        db.add(snap)
        snapshots.append(snap)
    
    db.commit()
    return snapshots
