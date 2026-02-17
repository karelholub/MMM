from __future__ import annotations

from typing import Any, Dict, Optional, Set, Tuple


SUPPORTED_KPIS = {"spend", "conversions", "revenue", "cpa", "roas"}


def safe_ratio(numerator: float, denominator: float) -> Optional[float]:
    if denominator and denominator > 0:
        return numerator / denominator
    return None


def metric_value(metric_row: Dict[str, float], kpi_key: str) -> Optional[float]:
    spend = float(metric_row.get("spend", 0.0) or 0.0)
    conversions = float(metric_row.get("conversions", 0.0) or 0.0)
    revenue = float(metric_row.get("revenue", 0.0) or 0.0)
    if kpi_key == "spend":
        return spend
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


def journey_revenue_value(
    journey: Dict[str, Any],
    *,
    dedupe_seen: Optional[Set[str]] = None,
) -> float:
    entries = journey.get("_revenue_entries")
    if not isinstance(entries, list):
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
            total += float(entry.get("value_in_base") or 0.0)
        except Exception:
            continue
    return total


def summarize_rows(rows: Dict[str, Dict[str, float]]) -> Tuple[Dict[str, float], Dict[str, Optional[float]]]:
    totals = {"spend": 0.0, "conversions": 0.0, "revenue": 0.0}
    for row in rows.values():
        totals["spend"] += float(row.get("spend", 0.0) or 0.0)
        totals["conversions"] += float(row.get("conversions", 0.0) or 0.0)
        totals["revenue"] += float(row.get("revenue", 0.0) or 0.0)
    return totals, derive_efficiency(
        spend=totals["spend"],
        conversions=totals["conversions"],
        revenue=totals["revenue"],
    )
