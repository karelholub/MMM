"""
Incrementality experiment automation service.

Features:
- Automated assignment: hash-based deterministic assignment for stable control/treatment groups
- Exposure tracking: log when profiles are exposed to treatment (e.g., message sent)
- Nightly reporting: scheduled job to compute daily snapshots and send alerts
- Power analysis: estimate required sample size for target MDE
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from .models_config_dq import (
    Experiment,
    ExperimentAssignment,
    ExperimentExposure,
    ExperimentOutcome,
    ExperimentResult,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Deterministic assignment
# ---------------------------------------------------------------------------


def deterministic_assignment(
    profile_id: str,
    experiment_id: int,
    treatment_rate: float = 0.5,
    salt: str = "",
) -> str:
    """
    Hash-based deterministic assignment.

    Returns "treatment" or "control" based on hash(profile_id + experiment_id + salt).
    This ensures:
    - Same profile always gets same group for a given experiment
    - No need to store assignments before exposure (lazy assignment)
    - Stable across restarts and distributed systems
    """
    key = f"{profile_id}:{experiment_id}:{salt}"
    h = hashlib.sha256(key.encode("utf-8")).digest()
    # Use first 8 bytes as uint64
    val = int.from_bytes(h[:8], byteorder="big")
    # Map to [0, 1)
    ratio = val / (2**64)
    return "treatment" if ratio < treatment_rate else "control"


def assign_profiles_deterministic(
    db: Session,
    experiment_id: int,
    profile_ids: List[str],
    treatment_rate: float = 0.5,
    salt: str = "",
    force_reassign: bool = False,
) -> Dict[str, int]:
    """
    Assign profiles to treatment/control using deterministic hashing.

    Parameters
    ----------
    db : database session
    experiment_id : experiment ID
    profile_ids : list of profile IDs to assign
    treatment_rate : fraction assigned to treatment (0.0 - 1.0)
    salt : optional salt for hash (use to re-randomize if needed)
    force_reassign : if True, overwrite existing assignments

    Returns
    -------
    {"treatment": count, "control": count}
    """
    if not profile_ids:
        return {"treatment": 0, "control": 0}

    # De-duplicate
    unique_ids = list(dict.fromkeys(profile_ids))

    # Check existing
    existing = {
        a.profile_id: a
        for a in db.query(ExperimentAssignment)
        .filter(ExperimentAssignment.experiment_id == experiment_id)
        .filter(ExperimentAssignment.profile_id.in_(unique_ids))
        .all()
    }

    counts = {"treatment": 0, "control": 0}
    now = datetime.utcnow()

    for pid in unique_ids:
        group = deterministic_assignment(pid, experiment_id, treatment_rate, salt)

        if pid in existing:
            if force_reassign:
                existing[pid].group = group
                existing[pid].assigned_at = now
        else:
            db.add(
                ExperimentAssignment(
                    experiment_id=experiment_id,
                    profile_id=pid,
                    group=group,
                    assigned_at=now,
                )
            )
        counts[group] += 1

    db.commit()
    return counts


# ---------------------------------------------------------------------------
# Exposure tracking
# ---------------------------------------------------------------------------


def record_exposure(
    db: Session,
    experiment_id: int,
    profile_id: str,
    exposure_ts: Optional[datetime] = None,
    campaign_id: Optional[str] = None,
    message_id: Optional[str] = None,
) -> None:
    """
    Record that a profile was exposed to the treatment (e.g., message sent).

    Exposures are logged separately from assignments to track actual delivery.
    """
    if exposure_ts is None:
        exposure_ts = datetime.utcnow()

    db.add(
        ExperimentExposure(
            experiment_id=experiment_id,
            profile_id=profile_id,
            exposure_ts=exposure_ts,
            campaign_id=campaign_id,
            message_id=message_id,
        )
    )
    db.commit()


def record_exposures_batch(
    db: Session,
    experiment_id: int,
    exposures: List[Dict[str, Any]],
) -> int:
    """
    Batch record exposures.

    Each exposure dict should have:
    - profile_id: str
    - exposure_ts: datetime (optional, defaults to now)
    - campaign_id: str (optional)
    - message_id: str (optional)

    Returns count of exposures recorded.
    """
    if not exposures:
        return 0

    now = datetime.utcnow()
    for exp in exposures:
        db.add(
            ExperimentExposure(
                experiment_id=experiment_id,
                profile_id=exp["profile_id"],
                exposure_ts=exp.get("exposure_ts", now),
                campaign_id=exp.get("campaign_id"),
                message_id=exp.get("message_id"),
            )
        )

    db.commit()
    return len(exposures)


# ---------------------------------------------------------------------------
# Outcome tracking
# ---------------------------------------------------------------------------


def record_outcome(
    db: Session,
    experiment_id: int,
    profile_id: str,
    conversion_ts: datetime,
    value: float = 0.0,
) -> None:
    """Record a conversion outcome for a profile in an experiment."""
    db.add(
        ExperimentOutcome(
            experiment_id=experiment_id,
            profile_id=profile_id,
            conversion_ts=conversion_ts,
            value=value,
        )
    )
    db.commit()


def record_outcomes_batch(
    db: Session,
    experiment_id: int,
    outcomes: List[Dict[str, Any]],
) -> int:
    """
    Batch record outcomes.

    Each outcome dict should have:
    - profile_id: str
    - conversion_ts: datetime
    - value: float (optional, defaults to 0.0)

    Returns count of outcomes recorded.
    """
    if not outcomes:
        return 0

    for out in outcomes:
        db.add(
            ExperimentOutcome(
                experiment_id=experiment_id,
                profile_id=out["profile_id"],
                conversion_ts=out["conversion_ts"],
                value=out.get("value", 0.0),
            )
        )

    db.commit()
    return len(outcomes)


# ---------------------------------------------------------------------------
# Results computation
# ---------------------------------------------------------------------------


def compute_experiment_results(
    db: Session,
    experiment_id: int,
    min_sample_size: int = 10,
) -> Dict[str, Any]:
    """
    Compute uplift results for an experiment.

    Returns dict with:
    - experiment_id
    - status
    - treatment: {n, conversions, conversion_rate, total_value}
    - control: {n, conversions, conversion_rate, total_value}
    - uplift_abs, uplift_rel, ci_low, ci_high, p_value
    - insufficient_data: bool
    """
    import math
    from math import erf

    exp = db.get(Experiment, experiment_id)
    if not exp:
        raise ValueError(f"Experiment {experiment_id} not found")

    assignments = (
        db.query(ExperimentAssignment)
        .filter(ExperimentAssignment.experiment_id == experiment_id)
        .all()
    )

    if not assignments:
        return {
            "experiment_id": experiment_id,
            "status": exp.status,
            "insufficient_data": True,
        }

    outcomes = {
        o.profile_id: o
        for o in db.query(ExperimentOutcome)
        .filter(ExperimentOutcome.experiment_id == experiment_id)
        .all()
    }

    treat_n = 0
    control_n = 0
    treat_conv = 0
    control_conv = 0
    treat_value = 0.0
    control_value = 0.0

    for a in assignments:
        out = outcomes.get(a.profile_id)
        if a.group == "treatment":
            treat_n += 1
            if out is not None:
                treat_conv += 1
                treat_value += float(out.value or 0.0)
        else:
            control_n += 1
            if out is not None:
                control_conv += 1
                control_value += float(out.value or 0.0)

    if treat_n < min_sample_size or control_n < min_sample_size:
        return {
            "experiment_id": experiment_id,
            "status": exp.status,
            "insufficient_data": True,
            "treatment": {"n": treat_n, "conversions": treat_conv},
            "control": {"n": control_n, "conversions": control_conv},
        }

    p_t = treat_conv / treat_n if treat_n > 0 else 0.0
    p_c = control_conv / control_n if control_n > 0 else 0.0
    diff = p_t - p_c
    uplift_abs = diff
    uplift_rel = diff / p_c if p_c > 0 else None

    # Normal-approx CI for diff in proportions
    se = math.sqrt(p_t * (1 - p_t) / treat_n + p_c * (1 - p_c) / control_n)
    if se > 0:
        z = 1.96
        ci_low = diff - z * se
        ci_high = diff + z * se
        z_score = diff / se
        p_value = 2 * (1 - 0.5 * (1 + erf(abs(z_score) / math.sqrt(2))))
    else:
        ci_low = None
        ci_high = None
        p_value = None

    # Upsert ExperimentResult
    res = (
        db.query(ExperimentResult)
        .filter(ExperimentResult.experiment_id == experiment_id)
        .first()
    )
    if res is None:
        res = ExperimentResult(
            experiment_id=experiment_id,
            computed_at=datetime.utcnow(),
            uplift_abs=uplift_abs,
            uplift_rel=uplift_rel,
            ci_low=ci_low,
            ci_high=ci_high,
            p_value=p_value,
            treatment_size=treat_n,
            control_size=control_n,
            meta_json={
                "treatment_conversions": treat_conv,
                "control_conversions": control_conv,
                "treatment_value": treat_value,
                "control_value": control_value,
            },
        )
        db.add(res)
    else:
        res.computed_at = datetime.utcnow()
        res.uplift_abs = uplift_abs
        res.uplift_rel = uplift_rel
        res.ci_low = ci_low
        res.ci_high = ci_high
        res.p_value = p_value
        res.treatment_size = treat_n
        res.control_size = control_n
        res.meta_json = {
            "treatment_conversions": treat_conv,
            "control_conversions": control_conv,
            "treatment_value": treat_value,
            "control_value": control_value,
        }

    db.commit()

    return {
        "experiment_id": experiment_id,
        "status": exp.status,
        "treatment": {
            "n": treat_n,
            "conversions": treat_conv,
            "conversion_rate": p_t,
            "total_value": treat_value,
        },
        "control": {
            "n": control_n,
            "conversions": control_conv,
            "conversion_rate": p_c,
            "total_value": control_value,
        },
        "uplift_abs": uplift_abs,
        "uplift_rel": uplift_rel,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "p_value": p_value,
        "insufficient_data": False,
    }


# ---------------------------------------------------------------------------
# Power analysis
# ---------------------------------------------------------------------------


def estimate_sample_size(
    baseline_rate: float,
    mde: float,
    alpha: float = 0.05,
    power: float = 0.8,
    treatment_rate: float = 0.5,
) -> int:
    """
    Estimate required sample size for a two-sample proportion test.

    Parameters
    ----------
    baseline_rate : control group conversion rate (e.g., 0.05 = 5%)
    mde : minimum detectable effect (absolute, e.g., 0.01 = 1pp)
    alpha : significance level (default 0.05)
    power : statistical power (default 0.8)
    treatment_rate : fraction in treatment (default 0.5 for balanced)

    Returns
    -------
    Total sample size (treatment + control)
    """
    import math

    # Two-sided test
    z_alpha = 1.96  # for alpha=0.05
    z_beta = 0.84  # for power=0.8

    p1 = baseline_rate
    p2 = baseline_rate + mde
    p_avg = (p1 + p2) / 2

    # Formula for two-proportion z-test
    n_per_group = (
        (z_alpha * math.sqrt(2 * p_avg * (1 - p_avg)) + z_beta * math.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2
    ) / (mde ** 2)

    n_treatment = n_per_group / treatment_rate
    n_control = n_per_group / (1 - treatment_rate)
    total = int(math.ceil(n_treatment + n_control))

    return total


# ---------------------------------------------------------------------------
# Nightly reporting
# ---------------------------------------------------------------------------


def run_nightly_report(
    db: Session,
    as_of_date: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Compute results for all running experiments and return summary.

    Intended to be called by a scheduled job (cron, Celery, etc.).

    Returns dict with:
    - as_of_date
    - experiments: list of {id, name, channel, status, results}
    - alerts: list of {experiment_id, message, severity}
    """
    if as_of_date is None:
        as_of_date = datetime.utcnow()

    # Find all running experiments
    running = (
        db.query(Experiment)
        .filter(Experiment.status == "running")
        .filter(Experiment.start_at <= as_of_date)
        .filter(Experiment.end_at >= as_of_date)
        .all()
    )

    results = []
    alerts = []

    for exp in running:
        try:
            res = compute_experiment_results(db, exp.id)
            results.append({
                "id": exp.id,
                "name": exp.name,
                "channel": exp.channel,
                "status": exp.status,
                "results": res,
            })

            # Generate alerts
            if res.get("insufficient_data"):
                alerts.append({
                    "experiment_id": exp.id,
                    "message": f"Experiment '{exp.name}' has insufficient data for uplift estimation",
                    "severity": "info",
                })
            elif res.get("p_value") is not None and res["p_value"] < 0.05:
                direction = "positive" if res["uplift_abs"] > 0 else "negative"
                alerts.append({
                    "experiment_id": exp.id,
                    "message": f"Experiment '{exp.name}' shows significant {direction} uplift (p={res['p_value']:.4f})",
                    "severity": "success" if direction == "positive" else "warning",
                })

        except Exception as e:
            logger.error(f"Failed to compute results for experiment {exp.id}: {e}")
            alerts.append({
                "experiment_id": exp.id,
                "message": f"Failed to compute results: {str(e)}",
                "severity": "error",
            })

    return {
        "as_of_date": as_of_date.isoformat(),
        "experiments": results,
        "alerts": alerts,
    }


def get_experiment_time_series(
    db: Session,
    experiment_id: int,
    freq: str = "D",
) -> pd.DataFrame:
    """
    Get daily/weekly time series of experiment metrics.

    Returns DataFrame with columns:
    - date
    - treatment_n, treatment_conversions, treatment_rate
    - control_n, control_conversions, control_rate
    - uplift_abs, uplift_rel
    """
    exp = db.get(Experiment, experiment_id)
    if not exp:
        raise ValueError(f"Experiment {experiment_id} not found")

    assignments = (
        db.query(ExperimentAssignment)
        .filter(ExperimentAssignment.experiment_id == experiment_id)
        .all()
    )

    if not assignments:
        return pd.DataFrame()

    outcomes = (
        db.query(ExperimentOutcome)
        .filter(ExperimentOutcome.experiment_id == experiment_id)
        .all()
    )

    # Build daily cumulative metrics
    assignment_dates = [a.assigned_at.date() for a in assignments]
    outcome_dates = [o.conversion_ts.date() for o in outcomes]

    if not assignment_dates:
        return pd.DataFrame()

    min_date = min(assignment_dates)
    max_date = max(assignment_dates + outcome_dates) if outcome_dates else min_date

    date_range = pd.date_range(start=min_date, end=max_date, freq="D")

    rows = []
    for d in date_range:
        # Assignments up to this date
        treat_n = sum(1 for a in assignments if a.group == "treatment" and a.assigned_at.date() <= d)
        control_n = sum(1 for a in assignments if a.group == "control" and a.assigned_at.date() <= d)

        # Outcomes up to this date
        assignment_pids = {a.profile_id: a.group for a in assignments if a.assigned_at.date() <= d}
        treat_conv = sum(
            1 for o in outcomes
            if o.conversion_ts.date() <= d and assignment_pids.get(o.profile_id) == "treatment"
        )
        control_conv = sum(
            1 for o in outcomes
            if o.conversion_ts.date() <= d and assignment_pids.get(o.profile_id) == "control"
        )

        treat_rate = treat_conv / treat_n if treat_n > 0 else 0.0
        control_rate = control_conv / control_n if control_n > 0 else 0.0
        uplift_abs = treat_rate - control_rate
        uplift_rel = uplift_abs / control_rate if control_rate > 0 else None

        rows.append({
            "date": d,
            "treatment_n": treat_n,
            "treatment_conversions": treat_conv,
            "treatment_rate": treat_rate,
            "control_n": control_n,
            "control_conversions": control_conv,
            "control_rate": control_rate,
            "uplift_abs": uplift_abs,
            "uplift_rel": uplift_rel,
        })

    df = pd.DataFrame(rows)

    if freq == "W":
        df["week"] = pd.to_datetime(df["date"]).dt.to_period("W").dt.start_time
        df = df.groupby("week").last().reset_index()
        df.rename(columns={"week": "date"}, inplace=True)

    return df


# ---------------------------------------------------------------------------
# Health checks / readiness
# ---------------------------------------------------------------------------


def compute_experiment_health(
    db: Session,
    experiment_id: int,
    min_assignments_per_group: int = 50,
    min_conversions_per_group: int = 20,
) -> Dict[str, Any]:
    """
    Compute high-level health signals for an experiment.

    Returns dict with:
    - experiment_id
    - sample: {"treatment": n, "control": n}
    - exposures: {"treatment": n, "control": n}
    - outcomes: {"treatment": n, "control": n}
    - balance: {"status": "ok"/"warn", "expected_share": float, "observed_share": float}
    - data_completeness: {
          "assignments": {"status": "ok"/"fail"},
          "outcomes": {"status": "ok"/"fail"},
          "exposures": {"status": "ok"/"warn"},
      }
    - overlap_risk: {"status": "ok"/"warn", "overlapping_profiles": int}
    - ready_state: {"label": "not_ready"/"early"/"ready", "reasons": [str]}
    """
    exp = db.get(Experiment, experiment_id)
    if not exp:
        raise ValueError(f"Experiment {experiment_id} not found")

    # Assignments and outcomes (reuse logic similar to compute_experiment_results)
    assignments = (
        db.query(ExperimentAssignment)
        .filter(ExperimentAssignment.experiment_id == experiment_id)
        .all()
    )
    assignment_groups = {a.profile_id: a.group for a in assignments}

    treat_n = sum(1 for a in assignments if a.group == "treatment")
    control_n = sum(1 for a in assignments if a.group == "control")

    outcomes_rows = (
        db.query(ExperimentOutcome)
        .filter(ExperimentOutcome.experiment_id == experiment_id)
        .all()
    )
    treat_conv = 0
    control_conv = 0
    for o in outcomes_rows:
        g = assignment_groups.get(o.profile_id)
        if g == "treatment":
            treat_conv += 1
        elif g == "control":
            control_conv += 1

    # Exposures joined with assignments (if any)
    exposures_rows = (
        db.query(ExperimentExposure)
        .filter(ExperimentExposure.experiment_id == experiment_id)
        .all()
    )
    treat_exp = 0
    control_exp = 0
    for e in exposures_rows:
        g = assignment_groups.get(e.profile_id)
        if g == "treatment":
            treat_exp += 1
        elif g == "control":
            control_exp += 1

    total_n = treat_n + control_n
    expected_share = 0.5  # default when specific split is not stored
    observed_share = (treat_n / total_n) if total_n > 0 else 0.0
    balance_status = "ok"
    if total_n > 0 and abs(observed_share - expected_share) > 0.1:
        balance_status = "warn"

    data_completeness = {
        "assignments": {"status": "ok" if total_n > 0 else "fail"},
        "outcomes": {"status": "ok" if (treat_conv + control_conv) > 0 else "fail"},
        "exposures": {
            "status": "ok" if (treat_exp + control_exp) > 0 else "warn"
        },
    }

    # Minimal contamination / overlap heuristic:
    # any profile assigned in this experiment also appearing in another running
    # experiment on the same channel with overlapping period.
    overlapping_profiles = 0
    if assignments:
        profile_ids = {a.profile_id for a in assignments}
        other_experiments = (
            db.query(Experiment)
            .filter(Experiment.id != experiment_id)
            .filter(Experiment.channel == exp.channel)
            .filter(Experiment.status == "running")
            .filter(Experiment.start_at <= exp.end_at)
            .filter(Experiment.end_at >= exp.start_at)
            .all()
        )
        if other_experiments:
            other_ids = [e.id for e in other_experiments]
            overlaps = (
                db.query(ExperimentAssignment.profile_id)
                .filter(ExperimentAssignment.experiment_id.in_(other_ids))
                .filter(ExperimentAssignment.profile_id.in_(profile_ids))
                .distinct()
                .all()
            )
            overlapping_profiles = len(overlaps)
    overlap_status = "ok" if overlapping_profiles == 0 else "warn"

    # Readiness classification
    reasons: List[str] = []
    if total_n < min_assignments_per_group * 2:
        reasons.append(
            f"Need at least {min_assignments_per_group} assignments per group; currently "
            f"{treat_n} treatment / {control_n} control."
        )
    if min(treat_conv, control_conv) < min_conversions_per_group:
        reasons.append(
            f"Need at least {min_conversions_per_group} conversions per group; currently "
            f"{treat_conv} treatment / {control_conv} control."
        )

    if not reasons:
        ready_label = "ready"
    elif min(treat_conv, control_conv) > 0:
        ready_label = "early"
    else:
        ready_label = "not_ready"

    return {
        "experiment_id": experiment_id,
        "sample": {"treatment": treat_n, "control": control_n},
        "exposures": {"treatment": treat_exp, "control": control_exp},
        "outcomes": {"treatment": treat_conv, "control": control_conv},
        "balance": {
            "status": balance_status,
            "expected_share": expected_share,
            "observed_share": observed_share,
        },
        "data_completeness": data_completeness,
        "overlap_risk": {
            "status": overlap_status,
            "overlapping_profiles": overlapping_profiles,
        },
        "ready_state": {"label": ready_label, "reasons": reasons},
    }


# ---------------------------------------------------------------------------
# Auto-assignment from conversion paths
# ---------------------------------------------------------------------------


def auto_assign_from_conversion_paths(
    db: Session,
    experiment_id: int,
    channel: str,
    start_date: datetime,
    end_date: datetime,
    treatment_rate: float = 0.5,
) -> Dict[str, int]:
    """
    Automatically assign profiles who had touchpoints in the experiment channel
    during the experiment period.

    Useful for post-hoc analysis of historical data.

    Returns {"treatment": count, "control": count}
    """
    from .models_config_dq import ConversionPath

    # Find all conversion paths with touchpoints in the channel during the period
    paths = (
        db.query(ConversionPath)
        .filter(ConversionPath.conversion_ts >= start_date)
        .filter(ConversionPath.conversion_ts <= end_date)
        .all()
    )

    eligible_profiles = set()
    for path in paths:
        path_json = path.path_json
        if not isinstance(path_json, dict):
            continue
        touchpoints = path_json.get("touchpoints", [])
        for tp in touchpoints:
            if tp.get("channel") == channel:
                eligible_profiles.add(path.profile_id)
                break

    return assign_profiles_deterministic(
        db=db,
        experiment_id=experiment_id,
        profile_ids=list(eligible_profiles),
        treatment_rate=treatment_rate,
    )
