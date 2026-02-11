"""
Build MMM weekly dataset from platform data: journeys (KPI) + expenses (spend).

Produces a wide-format DataFrame: date (week start), spend columns per channel, KPI column.
Stored as CSV and registered in DATASETS with metadata.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Week start: Monday (pandas 'W-MON')
WEEK_FREQ = "W-MON"


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
    """Aggregate KPI by week. kpi_target: 'sales' => sum(conversion_value), 'attribution' => count."""
    week_to_value: Dict[pd.Timestamp, float] = {}
    for j in journeys:
        if not j.get("converted", True):
            continue
        ts = _conversion_week_ts(j)
        if ts is None:
            continue
        # Week start (Monday)
        week_start = ts.normalize().to_period(WEEK_FREQ).start_time
        if date_start <= week_start <= date_end:
            if kpi_target == "sales":
                val = float(j.get("conversion_value") or 0)
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
    """Aggregate expenses by week and channel. Expects expense-like objects with channel, amount/converted_amount, service_period_start."""
    # Build (week_start, channel) -> amount
    week_channel: Dict[Tuple[pd.Timestamp, str], float] = {}
    for exp in expenses:
        ch = getattr(exp, "channel", None) or (exp.get("channel") if isinstance(exp, dict) else None)
        if not ch or ch not in spend_channels:
            continue
        status = getattr(exp, "status", "active") or (exp.get("status") if isinstance(exp, dict) else "active")
        if status == "deleted":
            continue
        amount = getattr(exp, "converted_amount", None) or getattr(exp, "amount", None)
        if amount is None and isinstance(exp, dict):
            amount = exp.get("converted_amount") or exp.get("amount")
        if amount is None:
            continue
        amount = float(amount)
        start_str = getattr(exp, "service_period_start", None) or (exp.get("service_period_start") if isinstance(exp, dict) else None)
        if not start_str:
            # Fallback to period YYYY-MM
            p = getattr(exp, "period", None) or (exp.get("period") if isinstance(exp, dict) else None)
            if p:
                start_str = f"{p}-01"
        if not start_str:
            continue
        try:
            ts = pd.to_datetime(start_str, errors="coerce")
            if pd.isna(ts):
                continue
            week_start = ts.normalize().to_period(WEEK_FREQ).start_time
            if date_start <= week_start <= date_end:
                key = (week_start, ch)
                week_channel[key] = week_channel.get(key, 0) + amount
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
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Build a wide-format MMM dataset from platform journeys and expenses.

    - date_start / date_end: ISO date strings (inclusive week range).
    - kpi_target: 'sales' (sum conversion_value) or 'attribution' (count conversions).
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
    date_start_ts = ds.normalize().to_period(WEEK_FREQ).start_time
    date_end_ts = de.normalize().to_period(WEEK_FREQ).start_time

    # Full week index
    week_range = pd.date_range(start=date_start_ts, end=date_end_ts, freq=WEEK_FREQ)
    n_weeks = len(week_range)

    # KPI by week
    kpi_series = _aggregate_kpi_by_week(journeys, kpi_target, date_start_ts, date_end_ts)
    kpi_col = "sales" if kpi_target == "sales" else "conversions"

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
    result[kpi_col] = result["date"].map(
        lambda d: float(kpi_series.get(pd.Timestamp(d).to_period(WEEK_FREQ).start_time, 0.0))
    )
    for cov in covariates:
        if cov not in result.columns:
            result[cov] = 0.0

    # Coverage
    missing_spend_weeks: Dict[str, int] = {}
    for ch in spend_channels:
        missing_spend_weeks[ch] = int((result[ch] == 0).sum())
    missing_kpi_weeks = int((result[kpi_col] == 0).sum())

    coverage = {
        "n_weeks": n_weeks,
        "missing_spend_weeks": missing_spend_weeks,
        "missing_kpi_weeks": missing_kpi_weeks,
    }
    return result, coverage
