"""
Smart mapping suggestions and validation for MMM datasets.

- Suggestion: date column, KPI (sales vs conversions), spend channels, covariates (binary / numeric).
- Validation: weekly sanity, non-negative spend, history length, multicollinearity, missingness.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Smart suggestions (schema inference)
# ---------------------------------------------------------------------------

DATE_KEYWORDS = ["date", "week", "period", "time", "ds"]
KPI_SALES_KEYWORDS = ["sales", "revenue", "orders", "profit", "aov", "value", "gmv"]
KPI_CONVERSIONS_KEYWORDS = ["conversions", "converted", "clicks", "signups", "leads", "count"]
SPEND_KEYWORDS = ["spend", "cost", "budget", "investment", "ad_spend", "media"]
CHANNEL_LIKE = ["google", "meta", "facebook", "linkedin", "tv", "radio", "email", "display"]
COV_BINARY_KEYWORDS = ["holiday", "promo", "event", "flag", "is_"]
COV_NUMERIC_KEYWORDS = ["price", "index", "competitor", "temperature", "season", "trend"]


def _is_numeric(dtype: str) -> bool:
    return dtype in ("float64", "int64", "float32", "int32")


def _col_score(name: str, keywords: List[str]) -> int:
    n = name.lower()
    return sum(1 for k in keywords if k in n)


def suggest_date_column(df: pd.DataFrame, columns: List[str]) -> Optional[str]:
    """Suggest date column (parsable, high parse rate, weekly-like)."""
    best_col = None
    best_score = 0
    for col in columns:
        try:
            parsed = pd.to_datetime(df[col], errors="coerce")
            rate = parsed.notna().sum() / len(df) if len(df) else 0
            if rate < 0.8:
                continue
            # Prefer column name hint
            score = _col_score(col, DATE_KEYWORDS) * 2 + (1 if rate > 0.95 else 0)
            if score > best_score:
                best_score = score
                best_col = col
        except Exception:
            continue
    return best_col


def suggest_kpi_columns(df: pd.DataFrame, columns: List[str]) -> Tuple[List[str], List[str]]:
    """Return (sales-like columns, conversions-like columns)."""
    numeric = [c for c in columns if _is_numeric(str(df[c].dtype))]
    sales = [c for c in numeric if _col_score(c, KPI_SALES_KEYWORDS) > 0]
    conv = [c for c in numeric if _col_score(c, KPI_CONVERSIONS_KEYWORDS) > 0]
    # If no keyword match, put all numeric (except obvious spend) in both
    spend_kw = set(SPEND_KEYWORDS + CHANNEL_LIKE)
    other_num = [c for c in numeric if not any(k in c.lower() for k in spend_kw)]
    if not sales:
        sales = other_num
    if not conv:
        conv = other_num
    return (sales[:10], conv[:10])


def suggest_spend_columns(df: pd.DataFrame, columns: List[str], exclude: Optional[List[str]] = None) -> List[str]:
    """Suggest spend columns by name patterns and numeric type."""
    exclude = set(exclude or [])
    numeric = [c for c in columns if _is_numeric(str(df[c].dtype)) and c not in exclude]
    scored = [(c, _col_score(c, SPEND_KEYWORDS) + _col_score(c, CHANNEL_LIKE)) for c in numeric]
    scored.sort(key=lambda x: -x[1])
    return [c for c, _ in scored if c not in exclude][:20]


def suggest_covariates(
    df: pd.DataFrame, columns: List[str], exclude: Optional[List[str]] = None
) -> Tuple[List[str], List[str]]:
    """Return (binary-like, numeric index-like) covariate columns."""
    exclude = set(exclude or [])
    remaining = [c for c in columns if c not in exclude and _is_numeric(str(df[c].dtype))]
    binary = [c for c in remaining if _col_score(c, COV_BINARY_KEYWORDS) > 0]
    numeric_cov = [c for c in remaining if _col_score(c, COV_NUMERIC_KEYWORDS) > 0 and c not in binary]
    return (binary[:10], numeric_cov[:10])


def build_smart_suggestions(
    df: pd.DataFrame,
    kpi_target: Optional[str] = None,
) -> Dict[str, Any]:
    """Build full suggestion set. kpi_target: 'sales' | 'attribution' | None (both)."""
    columns = list(df.columns)
    date_col = suggest_date_column(df, columns)
    kpi_sales, kpi_conv = suggest_kpi_columns(df, columns)
    kpi_columns = kpi_sales if kpi_target == "sales" else (kpi_conv if kpi_target == "attribution" else (kpi_sales + kpi_conv))
    if not kpi_columns:
        kpi_columns = [c for c in columns if _is_numeric(str(df[c].dtype))][:5]
    spend = suggest_spend_columns(df, columns, exclude=[date_col] if date_col else None)
    cov_binary, cov_numeric = suggest_covariates(
        df, columns, exclude=([date_col] if date_col else []) + spend + kpi_columns[:1]
    )
    covariates = cov_binary + cov_numeric
    return {
        "date_column": date_col,
        "kpi_columns": list(dict.fromkeys(kpi_columns)),
        "kpi_columns_sales": kpi_sales,
        "kpi_columns_conversions": kpi_conv,
        "spend_channels": spend,
        "covariates": covariates,
        "covariates_binary": cov_binary,
        "covariates_numeric": cov_numeric,
    }


# ---------------------------------------------------------------------------
# Validation (mapping applied)
# ---------------------------------------------------------------------------

MIN_WEEKS_WARNING = 52
MAX_GAP_WEEKS = 4
CORRELATION_THRESHOLD = 0.95
MISSING_PCT_ERROR = 50.0
MISSING_PCT_WARN = 10.0


def _parse_weeks(df: pd.DataFrame, date_column: str) -> Optional[pd.DatetimeIndex]:
    """Parse date column to weekly period (Monday). Returns None if invalid."""
    if date_column not in df.columns:
        return None
    try:
        s = pd.to_datetime(df[date_column], errors="coerce")
        s = s.dt.to_period("W-MON").dt.start_time
        return s.dropna().sort_values().unique()
    except Exception:
        return None


def validate_mapping(
    df: pd.DataFrame,
    date_column: str,
    kpi: str,
    spend_channels: List[str],
    covariates: Optional[List[str]] = None,
) -> Tuple[List[str], List[str], Dict[str, Any]]:
    """
    Validate mapping. Returns (errors, warnings, details).
    errors = blocking; warnings = can proceed.
    """
    errors: List[str] = []
    warnings: List[str] = []
    details: Dict[str, Any] = {"missingness": {}, "correlation_issues": [], "n_weeks": None}

    covariates = covariates or []
    # ---- Column presence ----
    for col in [date_column, kpi] + spend_channels + covariates:
        if col not in df.columns:
            errors.append(f"Column '{col}' is not in the dataset.")
    if errors:
        return (errors, warnings, details)

    # ---- Date / weekly ----
    weeks = _parse_weeks(df, date_column)
    if weeks is None or len(weeks) == 0:
        errors.append("Date column could not be parsed as weekly dates.")
    else:
        n_weeks = len(weeks)
        details["n_weeks"] = n_weeks
        if n_weeks < 12:
            errors.append(f"Too few distinct weeks ({n_weeks}). Need at least 12 for a minimal model.")
        elif n_weeks < MIN_WEEKS_WARNING:
            warnings.append(f"Only {n_weeks} weeks of history. For more stable MMM, 52+ weeks is recommended.")
        # Duplicate weeks (same week appears twice)
        week_series = pd.to_datetime(df[date_column], errors="coerce").dt.to_period("W-MON")
        week_counts = week_series.value_counts()
        week_counts = week_counts[week_counts.index.notna()]
        dup = week_counts[week_counts > 1]
        if len(dup) > 0:
            errors.append(f"Duplicate weeks detected: {len(dup)} week(s) have multiple rows. Use one row per week.")
        # Gaps
        weeks_series = pd.Series(weeks).sort_values()
        if len(weeks_series) > 1:
            diffs = weeks_series.diff().dropna()
            gap_days = (diffs > pd.Timedelta(days=7 * (1 + MAX_GAP_WEEKS))).sum()
            if gap_days > 0:
                warnings.append(f"Large gap(s) in weekly series: {int(gap_days)} gap(s) exceed {MAX_GAP_WEEKS} weeks.")

    # ---- Non-negative spend ----
    for ch in spend_channels:
        if ch not in df.columns:
            continue
        ser = pd.to_numeric(df[ch], errors="coerce").fillna(0)
        neg = (ser < 0).sum()
        if neg > 0:
            errors.append(f"Spend column '{ch}' has {int(neg)} negative value(s). All spend must be non-negative.")

    # ---- KPI non-negative (warning only) ----
    if kpi in df.columns:
        ser = pd.to_numeric(df[kpi], errors="coerce")
        neg = (ser < 0).sum()
        if neg > 0:
            warnings.append(f"KPI column '{kpi}' has {int(neg)} negative value(s).")

    # ---- Missingness ----
    for col in [kpi] + spend_channels + covariates:
        if col not in df.columns:
            continue
        missing = df[col].isna().sum()
        pct = 100.0 * missing / len(df) if len(df) else 0
        details["missingness"][col] = {"count": int(missing), "pct": round(pct, 1)}
        if pct >= MISSING_PCT_ERROR:
            errors.append(f"Column '{col}' has {pct:.0f}% missing values. Fix or drop.")
        elif pct >= MISSING_PCT_WARN:
            warnings.append(f"Column '{col}' has {pct:.1f}% missing values.")

    # ---- Multicollinearity (spend vs spend) ----
    spend_df = df[spend_channels].apply(pd.to_numeric, errors="coerce").fillna(0)
    if len(spend_channels) >= 2 and spend_df.shape[0] > 1:
        corr = spend_df.corr()
        for i, a in enumerate(spend_channels):
            for j, b in enumerate(spend_channels):
                if i >= j:
                    continue
                c = corr.loc[a, b]
                if abs(c) >= CORRELATION_THRESHOLD:
                    details["correlation_issues"].append({"pair": [a, b], "correlation": round(float(c), 3)})
                    warnings.append(f"High correlation between '{a}' and '{b}' ({c:.2f}). Consider dropping one.")

    return (errors, warnings, details)


def get_missingness_top_offenders(details: Dict[str, Any], top_n: int = 5) -> List[Dict[str, Any]]:
    """Return top columns by missing % for display."""
    missingness = details.get("missingness") or {}
    items = [{"column": k, "count": v["count"], "pct": v["pct"]} for k, v in missingness.items()]
    items.sort(key=lambda x: -x["pct"])
    return items[:top_n]
