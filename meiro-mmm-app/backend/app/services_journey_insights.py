from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models_config_dq import JourneyPathDaily
from app.services_journey_path_outputs import query_path_daily_outputs


def _normalize_steps(path_steps: Any) -> List[str]:
    if isinstance(path_steps, list):
        return [str(step).strip() for step in path_steps if str(step).strip()]
    if isinstance(path_steps, str):
        return [part.strip() for part in path_steps.split(">") if part.strip()]
    return []


def _confidence_label(support_count: int, baseline_rate: float, observed_rate: float) -> str:
    gap = abs(observed_rate - baseline_rate)
    if support_count >= 500 and gap >= 0.03:
        return "high"
    if support_count >= 150 and gap >= 0.015:
        return "medium"
    return "low"


def _sample_size_target(rate: float, support_count: int) -> int:
    baseline = max(rate, 0.01)
    target = int(max(500, support_count * 2, 16 * (1 / baseline)))
    return min(target, 200_000)


def _serialize_insight(
    *,
    kind: str,
    title: str,
    summary: str,
    severity: str,
    support_count: int,
    baseline_rate: float,
    observed_rate: float,
    evidence: List[str],
    suggested_hypothesis: Dict[str, Any],
) -> Dict[str, Any]:
    confidence = _confidence_label(support_count, baseline_rate, observed_rate)
    estimated_users = int(max(0, support_count * max(0.0, baseline_rate - observed_rate)))
    return {
        "id": f"{kind}:{suggested_hypothesis.get('trigger', {}).get('path_hash') or title}",
        "kind": kind,
        "title": title,
        "summary": summary,
        "severity": severity,
        "confidence": confidence,
        "support_count": support_count,
        "baseline_rate": round(baseline_rate, 4),
        "observed_rate": round(observed_rate, 4),
        "impact_estimate": {
            "direction": "positive",
            "magnitude": "high" if estimated_users >= 100 else "medium" if estimated_users >= 25 else "low",
            "estimated_users_affected": estimated_users,
        },
        "evidence": evidence,
        "suggested_hypothesis": suggested_hypothesis,
    }


def build_journey_insights(
    db: Session,
    *,
    journey_definition_id: str,
    date_from: date,
    date_to: date,
    mode: str = "conversion_only",
    channel_group: Optional[str] = None,
    campaign_id: Optional[str] = None,
    device: Optional[str] = None,
    country: Optional[str] = None,
    target_kpi: Optional[str] = None,
) -> Dict[str, Any]:
    q = query_path_daily_outputs(
        db,
        journey_definition_id=journey_definition_id,
        date_from=date_from,
        date_to=date_to,
        mode=mode,
        channel_group=channel_group,
        campaign_id=campaign_id,
        device=device,
        country=country,
    )
    rows = q.order_by(JourneyPathDaily.count_journeys.desc(), JourneyPathDaily.path_hash.asc()).all()
    if not rows:
        return {
            "items": [],
            "summary": {
                "paths_considered": 0,
                "journeys": 0,
                "conversions": 0,
                "baseline_conversion_rate": 0.0,
            },
        }

    summary_row = q.with_entities(
        func.sum(JourneyPathDaily.count_journeys),
        func.sum(JourneyPathDaily.count_conversions),
    ).first()
    total_journeys = int(summary_row[0] or 0)
    total_conversions = int(summary_row[1] or 0)
    baseline_rate = (total_conversions / total_journeys) if total_journeys > 0 else 0.0

    valid_rows = [row for row in rows if int(row.count_journeys or 0) > 0]
    high_support = [row for row in valid_rows if int(row.count_journeys or 0) >= 25]
    avg_path_length = (
        sum(int(row.path_length or 0) * int(row.count_journeys or 0) for row in valid_rows) / total_journeys
        if total_journeys > 0
        else 0.0
    )

    best_row = None
    for row in high_support:
        rate = (int(row.count_conversions or 0) / int(row.count_journeys or 0)) if int(row.count_journeys or 0) > 0 else 0.0
        if rate >= baseline_rate * 1.2:
            best_row = row
            break

    weakest_row = None
    for row in high_support:
        rate = (int(row.count_conversions or 0) / int(row.count_journeys or 0)) if int(row.count_journeys or 0) > 0 else 0.0
        if rate <= baseline_rate * 0.8:
            weakest_row = row
            break

    slow_row = None
    for row in sorted(high_support, key=lambda item: float(item.p50_time_to_convert_sec or 0), reverse=True):
        if float(row.p50_time_to_convert_sec or 0) > 0 and int(row.path_length or 0) >= avg_path_length:
            slow_row = row
            break

    items: List[Dict[str, Any]] = []

    if weakest_row is not None:
        support_count = int(weakest_row.count_journeys or 0)
        observed_rate = (int(weakest_row.count_conversions or 0) / support_count) if support_count > 0 else 0.0
        steps = _normalize_steps(weakest_row.path_steps)
        items.append(
            _serialize_insight(
                kind="low_conversion_path",
                title="Recover a high-volume low-converting path",
                summary=f"{' → '.join(steps[:4])} converts {max(0.0, (baseline_rate - observed_rate) * 100):.1f}pp below baseline.",
                severity="high",
                support_count=support_count,
                baseline_rate=baseline_rate,
                observed_rate=observed_rate,
                evidence=[
                    f"{support_count:,} journeys in the selected window.",
                    f"Observed conversion rate {observed_rate:.1%} vs baseline {baseline_rate:.1%}.",
                    f"Path length {int(weakest_row.path_length or 0)} steps.",
                ],
                suggested_hypothesis={
                    "title": f"Recover {' → '.join(steps[:2]) or 'low-converting path'}",
                    "hypothesis_text": "Introduce a targeted intervention after this path prefix to recover users before they exit.",
                    "trigger": {"path_hash": weakest_row.path_hash, "steps": steps},
                    "segment": {
                        "channel_group": weakest_row.channel_group,
                        "campaign_id": weakest_row.campaign_id,
                        "device": weakest_row.device,
                        "country": weakest_row.country,
                    },
                    "current_action": {"type": "observe_only"},
                    "proposed_action": {"type": "nba_intervention", "idea": "Test a stronger next action for this prefix."},
                    "target_kpi": target_kpi,
                    "support_count": support_count,
                    "baseline_rate": round(baseline_rate, 4),
                    "sample_size_target": _sample_size_target(baseline_rate, support_count),
                },
            )
        )

    if best_row is not None:
        support_count = int(best_row.count_journeys or 0)
        observed_rate = (int(best_row.count_conversions or 0) / support_count) if support_count > 0 else 0.0
        steps = _normalize_steps(best_row.path_steps)
        items.append(
            _serialize_insight(
                kind="high_conversion_path",
                title="Scale a strong converting path",
                summary=f"{' → '.join(steps[:4])} converts {max(0.0, (observed_rate - baseline_rate) * 100):.1f}pp above baseline.",
                severity="medium",
                support_count=support_count,
                baseline_rate=baseline_rate,
                observed_rate=observed_rate,
                evidence=[
                    f"{support_count:,} journeys in the selected window.",
                    f"Observed conversion rate {observed_rate:.1%} vs baseline {baseline_rate:.1%}.",
                    "Use this path as a candidate control or benchmark journey.",
                ],
                suggested_hypothesis={
                    "title": f"Replicate {' → '.join(steps[:2]) or 'high-converting path'}",
                    "hypothesis_text": "Replicate the conditions of this path for similar segments and measure whether it lifts conversion rate.",
                    "trigger": {"path_hash": best_row.path_hash, "steps": steps},
                    "segment": {
                        "channel_group": best_row.channel_group,
                        "campaign_id": best_row.campaign_id,
                        "device": best_row.device,
                        "country": best_row.country,
                    },
                    "current_action": {"type": "observe_only"},
                    "proposed_action": {"type": "path_replication", "idea": "Test whether similar users benefit from the same sequence."},
                    "target_kpi": target_kpi,
                    "support_count": support_count,
                    "baseline_rate": round(baseline_rate, 4),
                    "sample_size_target": _sample_size_target(observed_rate, support_count),
                },
            )
        )

    if slow_row is not None:
        support_count = int(slow_row.count_journeys or 0)
        observed_rate = (int(slow_row.count_conversions or 0) / support_count) if support_count > 0 else 0.0
        steps = _normalize_steps(slow_row.path_steps)
        p50_time = float(slow_row.p50_time_to_convert_sec or 0.0)
        items.append(
            _serialize_insight(
                kind="slow_path",
                title="Reduce delay in a slow-to-convert path",
                summary=f"{' → '.join(steps[:4])} takes a median {int(p50_time // 3600)}h to convert.",
                severity="medium",
                support_count=support_count,
                baseline_rate=baseline_rate,
                observed_rate=observed_rate,
                evidence=[
                    f"P50 time to convert {int(p50_time // 3600)}h.",
                    f"Path length {int(slow_row.path_length or 0)} vs weighted average {avg_path_length:.1f}.",
                    f"{support_count:,} journeys observed.",
                ],
                suggested_hypothesis={
                    "title": f"Shorten {' → '.join(steps[:2]) or 'slow path'}",
                    "hypothesis_text": "Introduce an earlier next best action to reduce time-to-convert for this path.",
                    "trigger": {"path_hash": slow_row.path_hash, "steps": steps},
                    "segment": {
                        "channel_group": slow_row.channel_group,
                        "campaign_id": slow_row.campaign_id,
                        "device": slow_row.device,
                        "country": slow_row.country,
                    },
                    "current_action": {"type": "observe_only"},
                    "proposed_action": {"type": "timing_test", "idea": "Test an earlier intervention for this path prefix."},
                    "target_kpi": target_kpi,
                    "support_count": support_count,
                    "baseline_rate": round(baseline_rate, 4),
                    "sample_size_target": _sample_size_target(observed_rate, support_count),
                },
            )
        )

    return {
        "items": items,
        "summary": {
            "paths_considered": len(valid_rows),
            "journeys": total_journeys,
            "conversions": total_conversions,
            "baseline_conversion_rate": round(baseline_rate, 4),
        },
    }
