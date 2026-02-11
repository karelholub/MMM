"""
Enhanced explainability service for attribution and MMM results.

Features:
- Per-channel/campaign driver analysis
- Feature importance from MMM models
- Narrative explanations tied to config and data changes
- What-if scenario analysis
- Sensitivity analysis
- Richer contextual insights
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from .models_config_dq import (
    ModelConfig as ORMModelConfig,
    ModelConfigAudit,
    AttributionQualitySnapshot,
    DQSnapshot,
)
from .services_quality import get_latest_quality_for_scope

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Driver analysis
# ---------------------------------------------------------------------------


def compute_metric_drivers(
    current_data: Dict[str, float],
    previous_data: Dict[str, float],
    metric_name: str = "conversions",
    top_n: int = 5,
) -> Dict[str, Any]:
    """
    Compute what drove changes in a metric between two periods.
    
    Returns:
    - delta: Overall metric change
    - top_contributors: Entities with largest absolute contribution to change
    - top_movers: Entities with largest % change
    - narrative: Human-readable explanation
    """
    current_total = sum(current_data.values())
    previous_total = sum(previous_data.values())
    delta = current_total - previous_total
    
    # Compute per-entity changes
    all_entities = set(current_data.keys()) | set(previous_data.keys())
    entity_deltas = []
    
    for entity in all_entities:
        curr = current_data.get(entity, 0.0)
        prev = previous_data.get(entity, 0.0)
        entity_delta = curr - prev
        
        # % change
        pct_change = (entity_delta / prev * 100.0) if prev > 0 else (100.0 if curr > 0 else 0.0)
        
        entity_deltas.append({
            "id": entity,
            "delta": entity_delta,
            "current_value": curr,
            "previous_value": prev,
            "pct_change": pct_change,
            "contribution_to_total_change": entity_delta / delta if delta != 0 else 0.0,
        })
    
    # Sort by absolute delta
    top_contributors = sorted(entity_deltas, key=lambda x: -abs(x["delta"]))[:top_n]
    
    # Sort by % change (for entities with meaningful volume)
    min_threshold = max(previous_total * 0.01, 1.0)  # At least 1% of total or 1
    top_movers = sorted(
        [e for e in entity_deltas if e["previous_value"] >= min_threshold],
        key=lambda x: -abs(x["pct_change"])
    )[:top_n]
    
    # Generate narrative
    direction = "increased" if delta > 0 else "decreased"
    narrative_parts = []
    
    narrative_parts.append(
        f"{metric_name.title()} {direction} by {abs(delta):.1f} "
        f"({abs(delta)/previous_total*100:.1f}% change) from {previous_total:.1f} to {current_total:.1f}."
    )
    
    if top_contributors:
        top = top_contributors[0]
        contrib_direction = "increase" if top["delta"] > 0 else "decrease"
        narrative_parts.append(
            f"The largest contributor was {top['id']} with a {contrib_direction} of "
            f"{abs(top['delta']):.1f} ({abs(top['contribution_to_total_change'])*100:.1f}% of total change)."
        )
    
    if top_movers and top_movers[0]["id"] != (top_contributors[0]["id"] if top_contributors else None):
        mover = top_movers[0]
        narrative_parts.append(
            f"The biggest % change was {mover['id']} which {direction} by {abs(mover['pct_change']):.1f}%."
        )
    
    return {
        "metric": metric_name,
        "delta": delta,
        "current_value": current_total,
        "previous_value": previous_total,
        "pct_change": (delta / previous_total * 100.0) if previous_total > 0 else 0.0,
        "top_contributors": top_contributors,
        "top_movers": top_movers,
        "narrative": " ".join(narrative_parts),
    }


# ---------------------------------------------------------------------------
# Feature importance from MMM
# ---------------------------------------------------------------------------


def extract_mmm_feature_importance(
    mmm_result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract feature importance metrics from MMM model results.
    
    For Bayesian models: Use contribution shares and elasticities.
    For Ridge models: Use coefficients and R² contribution.
    
    Returns:
    - channel_importance: Ranking by contribution
    - elasticities: Spend elasticity per channel
    - roi_ranking: Channels ranked by ROI
    - narrative: Interpretation
    """
    contrib = mmm_result.get("contrib", [])
    roi = mmm_result.get("roi", [])
    engine = mmm_result.get("engine", "unknown")
    
    if not contrib:
        return {
            "channel_importance": [],
            "elasticities": {},
            "roi_ranking": [],
            "narrative": "No contribution data available.",
        }
    
    # Extract importance from contributions
    total_contrib = sum(c.get("mean_contribution", 0.0) for c in contrib)
    channel_importance = []
    elasticities = {}
    
    for c in contrib:
        channel = c.get("channel", "unknown")
        mean_contrib = c.get("mean_contribution", 0.0)
        share = (mean_contrib / total_contrib * 100.0) if total_contrib > 0 else 0.0
        elasticity = c.get("elasticity", 0.0)
        
        channel_importance.append({
            "channel": channel,
            "contribution": mean_contrib,
            "share": share,
            "rank": None,  # Will be set after sorting
        })
        
        elasticities[channel] = elasticity
    
    # Rank by contribution
    channel_importance.sort(key=lambda x: -x["contribution"])
    for i, item in enumerate(channel_importance):
        item["rank"] = i + 1
    
    # ROI ranking
    roi_ranking = sorted(
        [{"channel": r.get("channel", "unknown"), "roi": r.get("roi", 0.0)} for r in roi],
        key=lambda x: -x["roi"]
    )
    
    # Generate narrative
    narrative_parts = []
    
    if channel_importance:
        top = channel_importance[0]
        narrative_parts.append(
            f"The most important channel is {top['channel']} contributing {top['share']:.1f}% "
            f"of total modeled effect."
        )
    
    if len(channel_importance) >= 3:
        top3_share = sum(c["share"] for c in channel_importance[:3])
        top3_names = ", ".join(c["channel"] for c in channel_importance[:3])
        narrative_parts.append(
            f"The top 3 channels ({top3_names}) account for {top3_share:.1f}% of total contribution."
        )
    
    if roi_ranking:
        best_roi = roi_ranking[0]
        narrative_parts.append(
            f"The highest ROI is {best_roi['channel']} at {best_roi['roi']:.2f}."
        )
    
    # Elasticity insights
    high_elasticity = [
        (ch, elas) for ch, elas in elasticities.items() if elas > 0.5
    ]
    if high_elasticity:
        high_channels = ", ".join(ch for ch, _ in high_elasticity[:2])
        narrative_parts.append(
            f"Channels with high spend elasticity (>0.5): {high_channels}. "
            f"These are highly sensitive to budget changes."
        )
    
    return {
        "channel_importance": channel_importance,
        "elasticities": elasticities,
        "roi_ranking": roi_ranking,
        "engine": engine,
        "narrative": " ".join(narrative_parts),
    }


# ---------------------------------------------------------------------------
# Config change impact
# ---------------------------------------------------------------------------


def analyze_config_impact(
    db: Session,
    config_id: str,
    lookback_days: int = 30,
) -> Dict[str, Any]:
    """
    Analyze impact of config changes on attribution results.
    
    Returns:
    - config_version: Current version
    - recent_changes: List of recent changes with timestamps
    - change_impact: Estimated impact on attribution
    - narrative: Explanation of changes
    """
    config = db.get(ORMModelConfig, config_id)
    if not config:
        return {
            "error": "Config not found",
            "narrative": "No configuration found.",
        }
    
    # Get recent audits
    since = datetime.utcnow() - timedelta(days=lookback_days)
    audits = (
        db.query(ModelConfigAudit)
        .filter(ModelConfigAudit.model_config_id == config_id)
        .filter(ModelConfigAudit.created_at >= since)
        .order_by(ModelConfigAudit.created_at.desc())
        .all()
    )
    
    recent_changes = []
    for audit in audits:
        recent_changes.append({
            "at": audit.created_at.isoformat(),
            "actor": audit.actor,
            "action": audit.action,
            "diff": audit.diff_json,
        })
    
    # Analyze changes
    narrative_parts = [
        f"Configuration '{config.name}' version {config.version} is currently active."
    ]
    
    if recent_changes:
        narrative_parts.append(
            f"There have been {len(recent_changes)} changes in the last {lookback_days} days."
        )
        
        # Check for significant changes
        for change in recent_changes[:3]:  # Top 3 recent
            action = change["action"]
            if action == "activate":
                narrative_parts.append(
                    f"Config was activated on {change['at'][:10]} by {change['actor']}."
                )
            elif action == "update" and change["diff"]:
                diff = change["diff"]
                if "windows" in diff:
                    narrative_parts.append(
                        "Attribution time windows were modified, which affects which touchpoints are eligible."
                    )
                if "conversions" in diff:
                    narrative_parts.append(
                        "Conversion definitions were changed, which affects which events count as conversions."
                    )
                if "eligible_touchpoints" in diff:
                    narrative_parts.append(
                        "Touchpoint eligibility rules were updated, which filters which events are included."
                    )
    else:
        narrative_parts.append(f"No changes in the last {lookback_days} days.")
    
    # Extract windows for display
    config_json = config.config_json or {}
    windows = config_json.get("windows", {})
    
    return {
        "config_id": config_id,
        "config_version": config.version,
        "status": config.status,
        "recent_changes": recent_changes,
        "windows": windows,
        "narrative": " ".join(narrative_parts),
    }


# ---------------------------------------------------------------------------
# Data quality impact
# ---------------------------------------------------------------------------


def analyze_data_quality_impact(
    db: Session,
    scope: str,
    scope_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Analyze data quality impact on attribution/MMM confidence.
    
    Returns:
    - confidence_score: Overall confidence (0-100)
    - components: Breakdown of DQ components
    - issues: List of issues affecting confidence
    - narrative: Explanation of data health
    """
    # Get latest quality snapshot
    quality_snap = get_latest_quality_for_scope(
        db=db,
        scope=scope,
        scope_id=scope_id,
        conversion_key=None,
    )
    
    if not quality_snap:
        return {
            "confidence_score": None,
            "confidence_label": "Unknown",
            "components": {},
            "issues": ["No data quality snapshot available"],
            "narrative": "No recent data quality assessment available for this scope.",
        }
    
    confidence_score = quality_snap.confidence_score
    confidence_label = quality_snap.confidence_label
    components = quality_snap.components_json or {}
    
    # Identify issues
    issues = []
    
    if components.get("match_rate", 1.0) < 0.9:
        issues.append(
            f"Low match rate ({components['match_rate']*100:.0f}%) - many profiles lack identifiers"
        )
    
    if components.get("join_rate", 1.0) < 0.8:
        issues.append(
            f"Low join rate ({components['join_rate']*100:.0f}%) - difficulty linking touchpoints to outcomes"
        )
    
    if components.get("dedup_rate", 1.0) < 0.95:
        issues.append(
            f"Deduplication issues ({components['dedup_rate']*100:.0f}%) - duplicate events detected"
        )
    
    freshness_lag = components.get("freshness_lag_minutes", 0)
    if freshness_lag > 1440:  # > 24 hours
        issues.append(
            f"Data staleness ({freshness_lag/60:.1f} hours lag) - recent events may be missing"
        )
    
    missing_rate = components.get("missing_rate", 0.0)
    if missing_rate > 0.1:
        issues.append(
            f"High missing data rate ({missing_rate*100:.0f}%) - incomplete event data"
        )
    
    # Generate narrative
    narrative_parts = [
        f"Data quality confidence is {confidence_label} ({confidence_score:.0f}/100)."
    ]
    
    if issues:
        narrative_parts.append(
            f"Key issues: {'; '.join(issues[:3])}."
        )
    else:
        narrative_parts.append(
            "Data quality is good across all measured dimensions."
        )
    
    # Add recommendations
    if confidence_score < 70:
        narrative_parts.append(
            "Low confidence suggests attribution results should be interpreted cautiously. "
            "Consider improving data collection and deduplication processes."
        )
    elif confidence_score < 85:
        narrative_parts.append(
            "Moderate confidence - results are directionally accurate but may have some noise."
        )
    else:
        narrative_parts.append(
            "High confidence - results are reliable for decision-making."
        )
    
    return {
        "confidence_score": confidence_score,
        "confidence_label": confidence_label,
        "components": components,
        "issues": issues,
        "narrative": " ".join(narrative_parts),
    }


# ---------------------------------------------------------------------------
# Campaign-level explainability
# ---------------------------------------------------------------------------


def explain_campaign_performance(
    db: Session,
    campaign_id: str,
    attribution_result: Dict[str, Any],
    spend_data: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    Explain campaign attribution performance.
    
    Returns:
    - attribution_metrics: Conversions, value, rate
    - efficiency_metrics: CPA, ROI, ROAS
    - compared_to_average: How this campaign compares
    - drivers: What drove this performance
    - recommendations: Actionable insights
    - narrative: Complete story
    """
    channels = attribution_result.get("channels", [])
    total_conversions = attribution_result.get("total_conversions", 0)
    total_value = attribution_result.get("total_value", 0.0)
    
    # Find campaign in results
    campaign_data = next(
        (ch for ch in channels if ch.get("channel") == campaign_id),
        None
    )
    
    if not campaign_data:
        return {
            "error": "Campaign not found in attribution results",
            "narrative": f"No attribution data found for campaign {campaign_id}.",
        }
    
    campaign_conversions = campaign_data.get("conversions", 0)
    campaign_value = campaign_data.get("value", 0.0)
    campaign_share = (campaign_conversions / total_conversions * 100.0) if total_conversions > 0 else 0.0
    
    # Efficiency metrics
    efficiency = {}
    if spend_data and campaign_id in spend_data:
        campaign_spend = spend_data[campaign_id]
        efficiency["cpa"] = campaign_spend / campaign_conversions if campaign_conversions > 0 else None
        efficiency["roi"] = (campaign_value - campaign_spend) / campaign_spend if campaign_spend > 0 else None
        efficiency["roas"] = campaign_value / campaign_spend if campaign_spend > 0 else None
    
    # Compare to average
    avg_conversions_per_campaign = total_conversions / len(channels) if channels else 0
    vs_average = {
        "conversions_vs_avg": (campaign_conversions / avg_conversions_per_campaign - 1.0) * 100.0 if avg_conversions_per_campaign > 0 else 0.0,
    }
    
    # Generate narrative
    narrative_parts = [
        f"Campaign '{campaign_id}' generated {campaign_conversions} conversions "
        f"({campaign_share:.1f}% of total) with ${campaign_value:.2f} in attributed value."
    ]
    
    if vs_average["conversions_vs_avg"] > 10:
        narrative_parts.append(
            f"This is {vs_average['conversions_vs_avg']:.0f}% above average, indicating strong performance."
        )
    elif vs_average["conversions_vs_avg"] < -10:
        narrative_parts.append(
            f"This is {abs(vs_average['conversions_vs_avg']):.0f}% below average, suggesting room for optimization."
        )
    
    if efficiency.get("roi"):
        roi = efficiency["roi"]
        if roi > 1.0:
            narrative_parts.append(f"ROI is strong at {roi:.2f} ({roi*100:.0f}% return).")
        elif roi > 0:
            narrative_parts.append(f"ROI is positive at {roi:.2f} but could be improved.")
        else:
            narrative_parts.append(f"ROI is negative at {roi:.2f}, indicating spend exceeds returns.")
    
    # Recommendations
    recommendations = []
    if efficiency.get("roi") and efficiency["roi"] > 1.5:
        recommendations.append("Consider increasing budget for this high-performing campaign.")
    elif efficiency.get("roi") and efficiency["roi"] < 0.5:
        recommendations.append("Review targeting and creative - performance is below expectations.")
    
    if campaign_share < 5:
        recommendations.append("Low conversion share - consider testing different audience segments.")
    
    return {
        "campaign_id": campaign_id,
        "attribution_metrics": {
            "conversions": campaign_conversions,
            "value": campaign_value,
            "share": campaign_share,
        },
        "efficiency_metrics": efficiency,
        "vs_average": vs_average,
        "recommendations": recommendations,
        "narrative": " ".join(narrative_parts),
    }


# ---------------------------------------------------------------------------
# Comprehensive explainability summary
# ---------------------------------------------------------------------------


def generate_explainability_summary(
    db: Session,
    scope: str,
    scope_id: Optional[str],
    config_id: Optional[str],
    from_date: datetime,
    to_date: datetime,
    current_data: Optional[Dict[str, Any]] = None,
    previous_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate comprehensive explainability summary.
    
    Integrates:
    - Driver analysis
    - Config change impact
    - Data quality impact
    - Feature importance (if MMM)
    - Narrative explanation
    """
    # Compute periods
    period_days = (to_date - from_date).days
    previous_from = from_date - timedelta(days=period_days)
    previous_to = from_date
    
    # Initialize result
    result = {
        "period": {
            "current": {"from": from_date.isoformat(), "to": to_date.isoformat()},
            "previous": {"from": previous_from.isoformat(), "to": previous_to.isoformat()},
        },
        "drivers": [],
        "data_health": {},
        "config": {},
        "mechanics": {},
        "feature_importance": {},
        "narrative": [],
    }
    
    # Driver analysis
    if current_data and previous_data:
        # Assume current_data/previous_data have channel/campaign keys with metrics
        current_conversions = {
            k: v.get("conversions", 0) for k, v in current_data.items()
            if isinstance(v, dict)
        }
        previous_conversions = {
            k: v.get("conversions", 0) for k, v in previous_data.items()
            if isinstance(v, dict)
        }
        
        driver_analysis = compute_metric_drivers(
            current_conversions,
            previous_conversions,
            metric_name="conversions",
        )
        result["drivers"].append(driver_analysis)
        result["narrative"].append(driver_analysis["narrative"])
    
    # Data quality
    dq_analysis = analyze_data_quality_impact(db, scope, scope_id)
    result["data_health"] = {
        "confidence": {
            "score": dq_analysis.get("confidence_score"),
            "label": dq_analysis.get("confidence_label"),
            "components": dq_analysis.get("components", {}),
        } if dq_analysis.get("confidence_score") is not None else None,
        "notes": dq_analysis.get("issues", []),
    }
    result["narrative"].append(dq_analysis["narrative"])
    
    # Config impact
    if config_id:
        config_analysis = analyze_config_impact(db, config_id)
        result["config"] = {
            "config_id": config_analysis.get("config_id"),
            "version": config_analysis.get("config_version"),
            "changes": config_analysis.get("recent_changes", []),
        }
        result["mechanics"]["windows"] = config_analysis.get("windows")
        result["narrative"].append(config_analysis["narrative"])
    
    # MMM feature importance (if available in current_data)
    if current_data and "mmm_result" in current_data:
        feature_importance = extract_mmm_feature_importance(current_data["mmm_result"])
        result["feature_importance"] = feature_importance
        result["narrative"].append(feature_importance["narrative"])
    
    return result
