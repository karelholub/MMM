"""Confidence scoring and explainability helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from .models_config_dq import AttributionQualitySnapshot, DQSnapshot, ModelConfig, ModelConfigAudit
from .services_model_config import get_default_config_id


@dataclass
class ConfidenceComponents:
    match_rate: float
    join_rate: float
    missing_rate: float
    freshness_lag_minutes: float
    dedup_rate: float
    consent_share: float


def _label_from_score(score: float) -> str:
    if score >= 80:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def score_confidence(components: ConfidenceComponents) -> Tuple[float, str]:
    """Simple weighted scoring. 0–100."""
    # Clamp input
    m = max(0.0, min(1.0, components.match_rate))
    j = max(0.0, min(1.0, components.join_rate))
    miss = max(0.0, min(1.0, components.missing_rate))
    dup = max(0.0, min(1.0, components.dedup_rate))
    consent = max(0.0, min(1.0, components.consent_share))
    # Freshness penalty: 0–1 based on lag vs 24h
    lag_factor = max(0.0, min(1.0, 1.0 - components.freshness_lag_minutes / (24 * 60.0)))

    # Positive contributors
    score_pos = (
        0.3 * m +
        0.2 * j +
        0.15 * lag_factor +
        0.2 * consent
    )
    # Penalties (higher missing/dup -> lower score)
    penalty = 0.1 * miss + 0.05 * dup

    raw = (score_pos - penalty) * 100.0
    score = max(0.0, min(100.0, raw))
    return score, _label_from_score(score)


def compute_quality_snapshot_for_scope(
    db: Session,
    ts_bucket: datetime,
    scope: str,
    scope_id: Optional[str],
    conversion_key: Optional[str],
    dq_components: Dict[str, float],
) -> AttributionQualitySnapshot:
    comps = ConfidenceComponents(
        match_rate=dq_components.get("match_rate", 0.0),
        join_rate=dq_components.get("join_rate", 0.0),
        missing_rate=dq_components.get("missing_rate", 0.0),
        freshness_lag_minutes=dq_components.get("freshness_lag_minutes", 0.0),
        dedup_rate=dq_components.get("dedup_rate", 0.0),
        consent_share=dq_components.get("consent_share", 0.0),
    )
    score, label = score_confidence(comps)
    snap = AttributionQualitySnapshot(
        ts_bucket=ts_bucket,
        scope=scope,
        scope_id=scope_id,
        conversion_key=conversion_key,
        confidence_score=score,
        confidence_label=label,
        components_json={
            "match_rate": comps.match_rate,
            "join_rate": comps.join_rate,
            "missing_rate": comps.missing_rate,
            "freshness_lag_minutes": comps.freshness_lag_minutes,
            "dedup_rate": comps.dedup_rate,
            "consent_share": comps.consent_share,
        },
    )
    db.add(snap)
    return snap


def get_latest_quality_for_scope(
    db: Session,
    scope: str,
    scope_id: Optional[str],
    conversion_key: Optional[str],
) -> Optional[AttributionQualitySnapshot]:
    """Return the most recent confidence snapshot.

    If no row exists for a specific scope_id, gracefully fall back to a
    global snapshot for that scope (scope_id IS NULL).
    """
    def _query(for_scope_id: Optional[str]) -> Optional[AttributionQualitySnapshot]:
        q = db.query(AttributionQualitySnapshot).filter(
            AttributionQualitySnapshot.scope == scope
        )
        if for_scope_id is not None:
            q = q.filter(AttributionQualitySnapshot.scope_id == for_scope_id)
        if conversion_key:
            q = q.filter(AttributionQualitySnapshot.conversion_key == conversion_key)
        return (
            q.order_by(AttributionQualitySnapshot.ts_bucket.desc())
            .limit(1)
            .one_or_none()
        )

    snap = _query(scope_id)
    if snap is None and scope_id is not None:
        snap = _query(None)
    return snap


def compute_overall_quality_from_dq(db: Session) -> List[AttributionQualitySnapshot]:
    """Derive a coarse confidence snapshot for channel/campaign from latest DQ metrics.

    This uses aggregate DQ snapshots (freshness + completeness) to produce a
    global confidence score per scope (scope_id=None). Dashboards for specific
    channels/campaigns will fall back to this when no per-entity snapshot exists.
    """
    # Latest DQ bucket
    latest = (
        db.query(DQSnapshot.ts_bucket)
        .order_by(DQSnapshot.ts_bucket.desc())
        .first()
    )
    if not latest:
        return []
    ts_bucket = latest[0]

    snaps = (
        db.query(DQSnapshot)
        .filter(DQSnapshot.ts_bucket == ts_bucket)
        .all()
    )
    if not snaps:
        return []

    # Aggregate components
    dq_components: Dict[str, float] = {
        "match_rate": 1.0,
        "join_rate": 1.0,
        "missing_rate": 0.0,
        "freshness_lag_minutes": 0.0,
        "dedup_rate": 1.0,
        "consent_share": 1.0,
    }

    # Use "journeys" metrics for missing/dup/join, and any freshness_* metrics
    missing_pct_vals: List[float] = []
    dup_pct_vals: List[float] = []
    join_pct_vals: List[float] = []
    freshness_vals: List[float] = []

    for s in snaps:
        if s.source == "journeys":
            if s.metric_key in ("missing_profile_pct", "missing_timestamp_pct", "missing_channel_pct"):
                missing_pct_vals.append(s.metric_value)
            elif s.metric_key == "duplicate_id_pct":
                dup_pct_vals.append(s.metric_value)
            elif s.metric_key == "conversion_attributable_pct":
                join_pct_vals.append(s.metric_value)
        if s.metric_key == "freshness_lag_minutes":
            freshness_vals.append(s.metric_value)

    if missing_pct_vals:
        dq_components["missing_rate"] = max(missing_pct_vals) / 100.0
    if dup_pct_vals:
        # treat duplicates as failure -> higher dup => lower dedup_rate
        avg_dup = sum(dup_pct_vals) / len(dup_pct_vals)
        dq_components["dedup_rate"] = max(0.0, 1.0 - avg_dup / 100.0)
    if join_pct_vals:
        avg_join = sum(join_pct_vals) / len(join_pct_vals)
        dq_components["join_rate"] = max(0.0, min(1.0, avg_join / 100.0))
    if freshness_vals:
        dq_components["freshness_lag_minutes"] = float(
            sum(freshness_vals) / len(freshness_vals)
        )

    # Approximate match_rate as 1 - missing_profile_pct
    profile_missing = next(
        (s.metric_value for s in snaps if s.source == "journeys" and s.metric_key == "missing_profile_pct"),
        None,
    )
    if profile_missing is not None:
        dq_components["match_rate"] = max(0.0, 1.0 - profile_missing / 100.0)

    # Attach to current default config (if any) so conversion_key lines up with dashboards
    cfg_id = get_default_config_id()
    conversion_key: Optional[str] = None
    if cfg_id:
        cfg = db.get(ModelConfig, cfg_id)
        if cfg and cfg.config_json:
            conversions = cfg.config_json.get("conversions", {})
            conversion_key = conversions.get("primary_conversion_key")

    created: List[AttributionQualitySnapshot] = []
    for scope in ("channel", "campaign"):
        snap = compute_quality_snapshot_for_scope(
            db=db,
            ts_bucket=ts_bucket,
            scope=scope,
            scope_id=None,
            conversion_key=conversion_key,
            dq_components=dq_components,
        )
        created.append(snap)

    db.commit()
    return created


def load_config_and_meta(
    db: Session, config_id: Optional[str]
) -> Tuple[Optional[ModelConfig], Optional[Dict[str, Any]]]:
    effective_id = config_id or get_default_config_id()
    if not effective_id:
        return None, None
    cfg = db.get(ModelConfig, effective_id)
    if not cfg:
        return None, None
    cfg_json = cfg.config_json or {}
    conversions = cfg_json.get("conversions", {}) or {}
    windows = cfg_json.get("windows", {}) or {}
    attribution = cfg_json.get("attribution", {}) or {}
    conversion_key = conversions.get("primary_conversion_key")
    time_window = {
        "click_lookback_days": windows.get("click_lookback_days"),
        "impression_lookback_days": windows.get("impression_lookback_days"),
        "session_timeout_minutes": windows.get("session_timeout_minutes"),
        "conversion_latency_days": windows.get("conversion_latency_days"),
    }
    elig = attribution.get("eligible_touchpoints", {}) or {}
    eligible_touchpoints = {
        "include_channels": elig.get("include_channels"),
        "exclude_channels": elig.get("exclude_channels"),
        "include_event_types": elig.get("include_event_types"),
        "exclude_event_types": elig.get("exclude_event_types"),
    }
    meta = {
        "config_id": cfg.id,
        "config_version": cfg.version,
        "conversion_key": conversion_key,
        "time_window": time_window,
        "eligible_touchpoints": eligible_touchpoints,
    }
    return cfg, meta


def summarize_config_changes(db: Session, cfg_id: str, since: datetime) -> List[Dict[str, Any]]:
    """Return a simple list of config change summaries since a given time."""
    audits = (
        db.query(ModelConfigAudit)
        .filter(ModelConfigAudit.model_config_id == cfg_id, ModelConfigAudit.created_at >= since)
        .order_by(ModelConfigAudit.created_at.asc())
        .all()
    )
    summaries: List[Dict[str, Any]] = []
    for a in audits:
        summaries.append(
            {
                "at": a.created_at.isoformat(),
                "actor": a.actor,
                "action": a.action,
            }
        )
    return summaries

