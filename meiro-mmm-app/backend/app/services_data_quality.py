"""Data quality snapshot computation and alert evaluation."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import json
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from .models_config_dq import (
    ConversionPath,
    DQSnapshot,
    DQAlertRule,
    DQAlert,
    NotificationEndpoint,
)

DATA_DIR = Path(__file__).resolve().parent / "data"


def _now() -> datetime:
    return datetime.utcnow()


def _load_journeys(db: Session) -> List[Dict[str, Any]]:
    """Load journeys for DQ checks, preferring the normalised ConversionPath store.

    Fallback to the legacy JSON file if no ConversionPath rows exist yet.
    """
    # Primary source: normalised conversion paths
    rows = (
        db.query(ConversionPath)
        .order_by(ConversionPath.conversion_ts.desc())
        .limit(10000)
        .all()
    )
    if rows:
        journeys: List[Dict[str, Any]] = []
        for r in rows:
            payload = r.path_json
            if isinstance(payload, dict):
                journeys.append(payload)
        if journeys:
            return journeys

    # Legacy fallback: raw profiles pushed from Meiro CDP webhook
    sample_file = DATA_DIR / "meiro_cdp_profiles.json"
    # This file contains raw profiles; for DQ we only need basic fields, so we can treat it as events.
    if not sample_file.exists():
        return []
    try:
        data = json.loads(sample_file.read_text())
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "profiles" in data:
            return data["profiles"]
        return []
    except Exception:
        return []


def compute_freshness() -> List[Tuple[str, str, float, Dict[str, Any]]]:
    """Freshness metrics by source: last_event_ts and lag in minutes/hours.

    Returns list of (source, metric_key, metric_value, meta_json)
    """
    metrics: List[Tuple[str, str, float, Dict[str, Any]]] = []
    now = _now()

    sources_files = {
        "meiro_web": DATA_DIR / "meiro_cdp.csv",
        "meta_cost": DATA_DIR / "meta_ads.csv",
        "google_ads_cost": DATA_DIR / "google_ads.csv",
        "linkedin_cost": DATA_DIR / "linkedin_ads.csv",
    }

    for source, path in sources_files.items():
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path)
            ts_col = None
            for cand in ("updated_time", "date", "timestamp", "event_date"):
                if cand in df.columns:
                    ts_col = cand
                    break
            if not ts_col:
                continue
            parsed = pd.to_datetime(df[ts_col], errors="coerce")
            if parsed.notna().sum() == 0:
                continue
            last_ts = parsed.max().to_pydatetime()
            lag_minutes = (now - last_ts).total_seconds() / 60.0
            lag_hours = lag_minutes / 60.0
            meta = {"last_event_ts": last_ts.isoformat(), "lag_minutes": lag_minutes, "lag_hours": lag_hours}
            metrics.append((source, "freshness_lag_minutes", float(lag_minutes), meta))
        except Exception:
            continue

    return metrics


def compute_journeys_completeness(journeys: List[Dict[str, Any]]) -> List[Tuple[str, str, float, Dict[str, Any]]]:
    """Completeness, duplication, joinability metrics over journeys."""
    if not journeys:
        return []
    n = len(journeys)
    missing_profile = 0
    missing_ts = 0
    missing_channel = 0
    unknown_channel = 0
    attrib_eligible = 0

    seen_ids = set()
    duplicate_ids = 0

    for j in journeys:
        cid = j.get("customer_id") or j.get("profile_id") or j.get("id") or (j.get("customer") or {}).get("id")
        if not cid:
            missing_profile += 1
        else:
            if cid in seen_ids:
                duplicate_ids += 1
            else:
                seen_ids.add(cid)
        tps = j.get("touchpoints") or []
        if not tps:
            missing_ts += 1
            missing_channel += 1
            continue
        any_ts = False
        any_channel = False
        any_non_direct = False
        for tp in tps:
            if tp.get("timestamp") or tp.get("ts"):
                any_ts = True
            ch = tp.get("channel")
            if ch:
                any_channel = True
                if ch == "unknown":
                    unknown_channel += 1
                if ch not in ("direct", "unknown"):
                    any_non_direct += 1
        if not any_ts:
            missing_ts += 1
        if not any_channel:
            missing_channel += 1
        if any_non_direct:
            attrib_eligible += 1

    metrics: List[Tuple[str, str, float, Dict[str, Any]]] = []
    def pct(x: int) -> float:
        return float(x) / float(n) * 100.0 if n else 0.0

    metrics.append(("journeys", "missing_profile_pct", pct(missing_profile), {"missing": missing_profile, "total": n}))
    metrics.append(("journeys", "missing_timestamp_pct", pct(missing_ts), {"missing": missing_ts, "total": n}))
    metrics.append(("journeys", "missing_channel_pct", pct(missing_channel), {"missing": missing_channel, "total": n}))
    metrics.append(("journeys", "duplicate_id_pct", pct(duplicate_ids), {"duplicates": duplicate_ids, "total": n}))
    metrics.append(("journeys", "unknown_channel_share_pct", pct(unknown_channel), {"unknown_channel_events": unknown_channel, "total_journeys": n}))
    metrics.append(("journeys", "conversion_attributable_pct", pct(attrib_eligible), {"attrib_eligible": attrib_eligible, "total": n}))

    return metrics


def compute_dq_snapshots(db: Session, journeys_override: Optional[List[Dict[str, Any]]] = None, include_taxonomy: bool = True) -> List[DQSnapshot]:
    """Compute and persist DQ snapshots for the current time bucket."""
    ts_bucket = _now().replace(minute=0, second=0, microsecond=0)
    metrics: List[Tuple[str, str, float, Dict[str, Any]]] = []

    metrics.extend(compute_freshness())
    journeys = journeys_override if journeys_override is not None else _load_journeys(db)
    metrics.extend(compute_journeys_completeness(journeys))

    snapshots: List[DQSnapshot] = []
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
    
    # Add taxonomy-specific DQ metrics
    if include_taxonomy and journeys:
        try:
            from .services_taxonomy import persist_taxonomy_dq_snapshots
            taxonomy_snapshots = persist_taxonomy_dq_snapshots(db, journeys)
            snapshots.extend(taxonomy_snapshots)
        except Exception as e:
            # Don't fail entire DQ computation if taxonomy fails
            import logging
            logging.getLogger(__name__).error(f"Failed to compute taxonomy DQ snapshots: {e}")
    
    db.commit()
    return snapshots


def _baseline_for_rule(db: Session, rule: DQAlertRule, since: datetime) -> Optional[float]:
    q = (
        db.query(DQSnapshot)
        .filter(DQSnapshot.metric_key == rule.metric_key, DQSnapshot.ts_bucket >= since)
    )
    if rule.source:
        q = q.filter(DQSnapshot.source == rule.source)
    rows = q.all()
    if not rows:
        return None
    return sum(r.metric_value for r in rows) / float(len(rows))


def evaluate_alert_rules(db: Session) -> List[DQAlert]:
    """Evaluate enabled rules against latest snapshots; create alerts."""
    now = _now()
    latest_bucket_row = (
        db.query(DQSnapshot.ts_bucket)
        .order_by(DQSnapshot.ts_bucket.desc())
        .first()
    )
    if not latest_bucket_row:
        return []
    latest_bucket = latest_bucket_row[0]

    # Load snapshots for latest bucket
    snaps_by_key: Dict[Tuple[str, str], DQSnapshot] = {}
    for s in db.query(DQSnapshot).filter(DQSnapshot.ts_bucket == latest_bucket).all():
        snaps_by_key[(s.source, s.metric_key)] = s

    rules = db.query(DQAlertRule).filter(DQAlertRule.is_enabled.is_(True)).all()
    created_alerts: List[DQAlert] = []
    for rule in rules:
        # Find matching snapshot
        for (source, metric_key), snap in snaps_by_key.items():
            if metric_key != rule.metric_key:
                continue
            if rule.source and rule.source != source:
                continue
            baseline = _baseline_for_rule(db, rule, since=now - timedelta(days=rule.lookback_period_days))
            val = snap.metric_value
            triggered = False
            if rule.threshold_type == "gt" and val > rule.threshold_value:
                triggered = True
            elif rule.threshold_type == "lt" and val < rule.threshold_value:
                triggered = True
            elif baseline is not None and rule.threshold_type == "abs_change" and abs(val - baseline) > rule.threshold_value:
                triggered = True
            elif baseline is not None and rule.threshold_type == "pct_change":
                if baseline != 0:
                    pct_change = (val - baseline) / baseline * 100.0
                    if abs(pct_change) > rule.threshold_value:
                        triggered = True
            if not triggered:
                continue

            msg = f"{rule.name}: metric {metric_key} for {source} = {val:.2f}"
            alert = DQAlert(
                rule_id=rule.id,
                triggered_at=now,
                ts_bucket=latest_bucket,
                metric_value=val,
                baseline_value=baseline,
                status="open",
                message=msg,
            )
            db.add(alert)
            created_alerts.append(alert)

            # Placeholder: webhook/email/slack notifications can be added by reading NotificationEndpoint
    db.commit()
    return created_alerts

