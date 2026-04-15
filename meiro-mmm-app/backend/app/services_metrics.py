from __future__ import annotations

from typing import Any, Dict, Optional, Set, Tuple


SUPPORTED_KPIS = {"spend", "visits", "conversions", "revenue", "cpa", "roas"}


def safe_ratio(numerator: float, denominator: float) -> Optional[float]:
    if denominator and denominator > 0:
        return numerator / denominator
    return None


def metric_value(metric_row: Dict[str, float], kpi_key: str) -> Optional[float]:
    spend = float(metric_row.get("spend", 0.0) or 0.0)
    visits = float(metric_row.get("visits", 0.0) or 0.0)
    conversions = float(metric_row.get("conversions", 0.0) or 0.0)
    revenue = float(metric_row.get("revenue", 0.0) or 0.0)
    if kpi_key == "spend":
        return spend
    if kpi_key == "visits":
        return visits
    if kpi_key == "conversions":
        return conversions
    if kpi_key == "revenue":
        return revenue
    if kpi_key == "cpa":
        return safe_ratio(spend, conversions)
    if kpi_key == "roas":
        return safe_ratio(revenue, spend)
    return None


def derive_efficiency(*, spend: float, conversions: float, revenue: float) -> Dict[str, Optional[float]]:
    return {
        "roas": safe_ratio(revenue, spend),
        "cpa": safe_ratio(spend, conversions),
        "roi": safe_ratio(revenue - spend, spend),
    }


def delta_pct(current: float, previous: float) -> float:
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return round((current - previous) / previous * 100.0, 1)


def journey_outcome_summary(journey: Dict[str, Any]) -> Dict[str, float]:
    entries = journey.get("_revenue_entries")
    if not isinstance(entries, list) or not entries:
        outcome = journey.get("conversion_outcome")
        if isinstance(outcome, dict):
            return {
                "gross_value": float(outcome.get("gross_value", 0.0) or 0.0),
                "net_value": float(outcome.get("net_value", outcome.get("gross_value", 0.0)) or 0.0),
                "refunded_value": float(outcome.get("refunded_value", 0.0) or 0.0),
                "cancelled_value": float(outcome.get("cancelled_value", 0.0) or 0.0),
                "gross_conversions": float(outcome.get("gross_conversions", 0.0) or 0.0),
                "net_conversions": float(outcome.get("net_conversions", outcome.get("gross_conversions", 0.0)) or 0.0),
                "invalid_leads": float(outcome.get("invalid_leads", 0.0) or 0.0),
                "valid_leads": float(outcome.get("valid_leads", 0.0) or 0.0),
            }
        gross = float(journey.get("conversion_value") or 0.0)
        converted = 1.0 if journey.get("converted", True) else 0.0
        return {
            "gross_value": gross,
            "net_value": gross,
            "refunded_value": 0.0,
            "cancelled_value": 0.0,
            "gross_conversions": converted,
            "net_conversions": converted,
            "invalid_leads": 0.0,
            "valid_leads": converted,
        }

    summary = {
        "gross_value": 0.0,
        "net_value": 0.0,
        "refunded_value": 0.0,
        "cancelled_value": 0.0,
        "gross_conversions": 0.0,
        "net_conversions": 0.0,
        "invalid_leads": 0.0,
        "valid_leads": 0.0,
    }
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if not any(key in entry for key in summary):
            summary["gross_conversions"] += 1.0
            summary["net_conversions"] += 1.0
        for key in tuple(summary.keys()):
            try:
                summary[key] += float(entry.get(key) or 0.0)
            except Exception:
                continue
    return summary


def journey_revenue_value(
    journey: Dict[str, Any],
    *,
    value_mode: str = "gross_only",
    dedupe_seen: Optional[Set[str]] = None,
) -> float:
    entries = journey.get("_revenue_entries")
    if not isinstance(entries, list) or not entries:
        outcome = journey.get("conversion_outcome")
        if isinstance(outcome, dict):
            key = "net_value" if value_mode == "net_only" else "gross_value"
            return float(outcome.get(key, outcome.get("gross_value", 0.0)) or 0.0)
        return float(journey.get("conversion_value") or 0.0)
    total = 0.0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        dedup_key = str(entry.get("dedup_key") or "")
        if dedupe_seen is not None and dedup_key:
            if dedup_key in dedupe_seen:
                continue
            dedupe_seen.add(dedup_key)
        try:
            if value_mode == "net_only":
                total += float(entry.get("net_value_in_base") or entry.get("value_in_base") or 0.0)
            else:
                total += float(entry.get("value_in_base") or 0.0)
        except Exception:
            continue
    return total


def summarize_rows(rows: Dict[str, Dict[str, float]]) -> Tuple[Dict[str, float], Dict[str, Optional[float]]]:
    totals = {"spend": 0.0, "visits": 0.0, "conversions": 0.0, "revenue": 0.0}
    for row in rows.values():
        totals["spend"] += float(row.get("spend", 0.0) or 0.0)
        totals["visits"] += float(row.get("visits", 0.0) or 0.0)
        totals["conversions"] += float(row.get("conversions", 0.0) or 0.0)
        totals["revenue"] += float(row.get("revenue", 0.0) or 0.0)
    return totals, derive_efficiency(
        spend=totals["spend"],
        conversions=totals["conversions"],
        revenue=totals["revenue"],
    )
