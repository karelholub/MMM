"""
Build MMM weekly dataset from platform data: journeys (KPI) + expenses (spend).

Produces a wide-format DataFrame: date (week start), spend columns per channel, KPI column.
Stored as CSV and registered in DATASETS with metadata.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from .services_metrics import journey_revenue_value
from .services_walled_garden import aggregate_synthetic_by_week_channel, is_walled_garden, normalize_channel, synthetic_column

# Week start: Monday. Pandas "W-MON" means weeks ending on Monday, so use
# explicit weekday arithmetic for bucket assignment.
WEEK_FREQ = "W-MON"


def _field(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _week_start(ts: Any) -> pd.Timestamp:
    value = pd.to_datetime(ts, errors="coerce")
    if pd.isna(value):
        return value
    if getattr(value, "tzinfo", None) is not None:
        value = value.tz_convert(None) if hasattr(value, "tz_convert") else value.tz_localize(None)
    normalized = value.normalize()
    return normalized - pd.Timedelta(days=int(normalized.weekday()))


def _conversion_week_ts(j: Dict[str, Any]) -> Optional[pd.Timestamp]:
    """Derive conversion date from last touchpoint timestamp. Returns None if not usable."""
    tps = j.get("touchpoints") or []
    if not tps:
        return None
    last_ts = None
    for tp in tps:
        ts = tp.get("timestamp")
        if not ts:
            continue
        try:
            t = pd.to_datetime(ts, errors="coerce")
            if pd.notna(t):
                last_ts = t if last_ts is None else max(last_ts, t)
        except Exception:
            continue
    return last_ts


def _aggregate_kpi_by_week(
    journeys: List[Dict[str, Any]],
    kpi_target: str,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
) -> pd.Series:
    """Aggregate KPI by week. kpi_target: 'sales' => sum(configured revenue), 'attribution' => count."""
    dedupe_seen: set[str] = set()
    week_to_value: Dict[pd.Timestamp, float] = {}
    for j in journeys:
        if not j.get("converted", True):
            continue
        ts = _conversion_week_ts(j)
        if ts is None:
            continue
        # Week start (Monday)
        week_start = _week_start(ts)
        if date_start <= week_start <= date_end:
            if kpi_target == "sales":
                val = journey_revenue_value(j, dedupe_seen=dedupe_seen)
            else:
                val = 1.0  # count conversions
            week_to_value[week_start] = week_to_value.get(week_start, 0) + val

    return pd.Series(week_to_value)


def _aggregate_expenses_by_week_channel(
    expenses: List[Any],
    spend_channels: List[str],
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
) -> pd.DataFrame:
    """Aggregate expenses by week and channel.

    Service-period expenses are allocated evenly across covered days, then
    rolled into Monday-start weeks. This keeps monthly spend usable for MMM
    windows that contain only part of the service period.
    """
    # Build (week_start, channel) -> amount
    week_channel: Dict[Tuple[pd.Timestamp, str], float] = {}
    selected_start = date_start.normalize()
    selected_end = date_end.normalize() + pd.Timedelta(days=6)
    for exp in expenses:
        ch = _field(exp, "channel")
        if not ch or ch not in spend_channels:
            continue
        status = _field(exp, "status", "active") or "active"
        if status == "deleted":
            continue
        amount = _field(exp, "converted_amount") or _field(exp, "amount")
        if amount is None:
            continue
        amount = float(amount)
        start_str = _field(exp, "service_period_start")
        end_str = _field(exp, "service_period_end")
        if not start_str:
            # Fallback to period YYYY-MM
            p = _field(exp, "period")
            if p:
                start_str = f"{p}-01"
                end_str = pd.Timestamp(start_str).to_period("M").end_time.strftime("%Y-%m-%d")
        if not start_str:
            continue
        try:
            service_start = pd.to_datetime(start_str, errors="coerce")
            service_end = pd.to_datetime(end_str, errors="coerce") if end_str else service_start
            if pd.isna(service_start) or pd.isna(service_end):
                continue
            service_start = service_start.normalize()
            service_end = service_end.normalize()
            if service_end < service_start:
                service_start, service_end = service_end, service_start
            total_days = max(int((service_end - service_start).days) + 1, 1)
            daily_amount = amount / total_days
            overlap_start = max(service_start, selected_start)
            overlap_end = min(service_end, selected_end)
            if overlap_end < overlap_start:
                continue
            for day in pd.date_range(overlap_start, overlap_end, freq="D"):
                week_start = _week_start(day)
                if not (date_start <= week_start <= date_end):
                    continue
                key = (week_start, ch)
                week_channel[key] = week_channel.get(key, 0) + daily_amount
        except Exception:
            continue

    if not week_channel:
        return pd.DataFrame(columns=["date"] + spend_channels)

    rows: List[Dict[str, Any]] = []
    for (week_start, ch), amount in week_channel.items():
        row = {"date": week_start, "channel": ch, "amount": amount}
        rows.append(row)
    df = pd.DataFrame(rows)
    pivot = df.pivot_table(index="date", columns="channel", values="amount", aggfunc="sum", fill_value=0.0)
    for ch in spend_channels:
        if ch not in pivot.columns:
            pivot[ch] = 0.0
    pivot = pivot.reindex(columns=spend_channels, fill_value=0.0)
    pivot.index.name = "date"
    return pivot.reset_index()


def build_mmm_dataset_from_platform(
    journeys: List[Dict[str, Any]],
    expenses: List[Any],
    date_start: str,
    date_end: str,
    kpi_target: str,
    spend_channels: List[str],
    covariates: Optional[List[str]] = None,
    currency: str = "USD",
    delivery_rows: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Build a wide-format MMM dataset from platform journeys and expenses.

    - date_start / date_end: ISO date strings (inclusive week range).
    - kpi_target: 'sales' (sum configured revenue) or 'attribution' (count conversions).
    - spend_channels: list of channel names matching expense.channel.
    - covariates: optional list; if provided we add placeholder columns (0) for now.

    Returns (df, coverage_dict).
    df has columns: date, <ch1>, <ch2>, ..., kpi_column (sales or conversions).
    coverage_dict: n_weeks, missing_spend_weeks per channel, missing_kpi_weeks.
    """
    covariates = covariates or []
    ds = pd.to_datetime(date_start, errors="coerce")
    de = pd.to_datetime(date_end, errors="coerce")
    if pd.isna(ds) or pd.isna(de) or ds > de:
        raise ValueError("Invalid date_start or date_end")
    # Align to week boundaries (Monday)
    date_start_ts = _week_start(ds)
    date_end_ts = _week_start(de)

    # Full week index
    week_range = pd.date_range(start=date_start_ts, end=date_end_ts, freq=WEEK_FREQ)
    n_weeks = len(week_range)

    # KPI by week
    kpi_series = _aggregate_kpi_by_week(journeys, kpi_target, date_start_ts, date_end_ts)
    kpi_col = "sales" if kpi_target == "sales" else "conversions"

    normalized_spend_channels = [normalize_channel(ch) for ch in spend_channels]

    # Spend by week and channel -> dict (date_str -> { channel -> amount })
    spend_df = _aggregate_expenses_by_week_channel(expenses, spend_channels, date_start_ts, date_end_ts)
    spend_by_date: Dict[str, Dict[str, float]] = {}
    if not spend_df.empty and "date" in spend_df.columns:
        for _, row in spend_df.iterrows():
            dt = row["date"]
            if hasattr(dt, "strftime"):
                d = dt.strftime("%Y-%m-%d")
            else:
                d = pd.to_datetime(dt).strftime("%Y-%m-%d")
            spend_by_date.setdefault(d, {})
            for ch in spend_channels:
                spend_by_date[d][ch] = float(row.get(ch, 0))
    for w in week_range:
        d = w.strftime("%Y-%m-%d")
        spend_by_date.setdefault(d, {})
        for ch in spend_channels:
            spend_by_date[d].setdefault(ch, 0.0)

    # Build wide table: one row per week
    result = pd.DataFrame({"date": [w.strftime("%Y-%m-%d") for w in week_range]})
    for ch in spend_channels:
        result[ch] = result["date"].map(lambda d: spend_by_date.get(d, {}).get(ch, 0.0))

    synthetic_details: Dict[str, Any] = {"channels": {}, "method": "synthetic_impressions_v1"}
    synthetic_df = pd.DataFrame()
    if delivery_rows:
        synthetic_df, synthetic_details = aggregate_synthetic_by_week_channel(
            delivery_rows,
            channels=normalized_spend_channels,
            week_start_fn=_week_start,
            date_start=date_start_ts,
            date_end=date_end_ts,
        )
    synthetic_by_date: Dict[str, Dict[str, float]] = {}
    if not synthetic_df.empty:
        for _, row in synthetic_df.iterrows():
            dt = row["date"]
            d = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else pd.to_datetime(dt).strftime("%Y-%m-%d")
            col = str(row.get("synthetic_column") or synthetic_column(row.get("channel")))
            synthetic_by_date.setdefault(d, {})
            synthetic_by_date[d][col] = synthetic_by_date[d].get(col, 0.0) + float(row.get("synthetic_impressions") or 0.0)
    synthetic_columns = [synthetic_column(ch) for ch in normalized_spend_channels if is_walled_garden(ch)]
    for col in synthetic_columns:
        result[col] = result["date"].map(lambda d, c=col: synthetic_by_date.get(d, {}).get(c, 0.0))

    result[kpi_col] = result["date"].map(
        lambda d: float(kpi_series.get(_week_start(pd.Timestamp(d)), 0.0))
    )
    for cov in covariates:
        if cov not in result.columns:
            result[cov] = 0.0

    spend_totals = {ch: float(result[ch].sum()) for ch in spend_channels}
    synthetic_totals = {col: float(result[col].sum()) for col in synthetic_columns}
    channels_with_spend = [ch for ch, value in spend_totals.items() if value > 0]
    all_zero_spend_channels = [ch for ch, value in spend_totals.items() if value <= 0]
    channels_with_synthetic_impressions = [
        source for source, col in zip(normalized_spend_channels, synthetic_columns) if synthetic_totals.get(col, 0.0) > 0
    ]
    total_spend = float(sum(spend_totals.values()))
    if spend_channels and total_spend <= 0:
        raise ValueError(
            "No spend was found for the selected MMM channels and date range. "
            "Choose channels with active expenses in this period or update Data Sources / Expenses."
        )

    # Coverage
    missing_spend_weeks: Dict[str, int] = {}
    for ch in spend_channels:
        missing_spend_weeks[ch] = int((result[ch] == 0).sum())
    missing_kpi_weeks = int((result[kpi_col] == 0).sum())

    coverage = {
        "n_weeks": n_weeks,
        "missing_spend_weeks": missing_spend_weeks,
        "missing_kpi_weeks": missing_kpi_weeks,
        "spend_totals": spend_totals,
        "synthetic_impression_totals": synthetic_totals,
        "synthetic_impression_columns": synthetic_columns,
        "channels_with_synthetic_impressions": channels_with_synthetic_impressions,
        "delivery": synthetic_details,
        "channels_with_spend": channels_with_spend,
        "all_zero_spend_channels": all_zero_spend_channels,
        "total_spend": total_spend,
    }
    return result, coverage
