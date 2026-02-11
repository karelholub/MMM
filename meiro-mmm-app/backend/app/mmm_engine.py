"""
Marketing Mix Modeling engine.

Primary: PyMC-Marketing Bayesian MMM (GeometricAdstock + LogisticSaturation).
Fallback: Ridge regression when pymc-marketing is not installed.
"""

import logging
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Feature flag – detect whether pymc-marketing is available at import time.
# ---------------------------------------------------------------------------
try:
    from pymc_marketing.mmm import (
        MMM,
        GeometricAdstock,
        LogisticSaturation,
    )
    import pymc as pm
    import arviz as az

    PYMC_AVAILABLE = True
    logger.info("pymc-marketing detected – Bayesian MMM engine enabled")
except ImportError:
    PYMC_AVAILABLE = False
    logger.info("pymc-marketing not installed – using Ridge fallback")


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def engine_info() -> Dict[str, Any]:
    """Return metadata about the active engine."""
    return {
        "engine": "pymc-marketing" if PYMC_AVAILABLE else "ridge-fallback",
        "pymc_available": PYMC_AVAILABLE,
    }


# ---------------------------------------------------------------------------
# Bayesian fitting (PyMC-Marketing)
# ---------------------------------------------------------------------------

def _fit_bayesian(
    df: pd.DataFrame,
    date_column: str,
    target_column: str,
    channel_columns: List[str],
    control_columns: List[str],
    adstock_cfg: Dict[str, Any],
    saturation_cfg: Dict[str, Any],
    mcmc_cfg: Dict[str, Any],
    random_seed: Optional[int] = None,
) -> Dict[str, Any]:
    """Fit a full Bayesian MMM via pymc-marketing and return results dict."""

    l_max = adstock_cfg.get("l_max", 8)
    seed = int(random_seed) if random_seed is not None else 42

    mmm = MMM(
        date_column=date_column,
        channel_columns=channel_columns,
        control_columns=control_columns if control_columns else None,
        adstock=GeometricAdstock(l_max=l_max),
        saturation=LogisticSaturation(),
    )

    draws = mcmc_cfg.get("draws", 1000)
    tune = mcmc_cfg.get("tune", 1000)
    chains = mcmc_cfg.get("chains", 4)
    target_accept = mcmc_cfg.get("target_accept", 0.9)

    X = df[[date_column] + channel_columns + control_columns].copy()
    y = df[target_column].values

    mmm.fit(
        X=X,
        y=y,
        draws=draws,
        tune=tune,
        chains=chains,
        target_accept=target_accept,
        random_seed=seed,
    )

    # --- Extract results ------------------------------------------------

    # Channel contributions (posterior mean per observation per channel)
    channel_contributions = mmm.compute_channel_contribution_original_scale()
    # channel_contributions shape: (draws*chains, n_obs, n_channels) → mean over samples
    mean_contributions = channel_contributions.mean(axis=(0, 1))  # per channel total mean
    total_contribution = float(mean_contributions.sum()) or 1.0

    # Posterior predictive for R²
    posterior_pred = mmm.sample_posterior_predictive(X)
    y_pred_mean = posterior_pred.posterior_predictive["y"].mean(dim=["chain", "draw"]).values
    ss_res = float(np.sum((y - y_pred_mean) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2)) or 1.0
    r2 = 1.0 - ss_res / ss_tot

    # Build contrib list
    contrib = []
    for i, ch in enumerate(channel_columns):
        share = float(mean_contributions[i]) / total_contribution
        contrib.append({"channel": ch, "beta": float(mean_contributions[i]), "mean_share": share})

    # ROI: total channel contribution / total channel spend
    roi = []
    for i, ch in enumerate(channel_columns):
        total_spend = float(df[ch].sum()) or 1e-6
        total_ch_contrib = float(channel_contributions.mean(axis=0)[:, i].sum())
        roi_val = total_ch_contrib / total_spend
        roi.append({"channel": ch, "roi": roi_val})

    # Adstock & saturation parameters (posterior means)
    adstock_params = {}
    saturation_params = {}
    try:
        posterior = mmm.fit_result.posterior
        for i, ch in enumerate(channel_columns):
            adstock_params[ch] = {
                "alpha": float(posterior["adstock_alpha"].mean(dim=["chain", "draw"]).values[i])
                if "adstock_alpha" in posterior
                else None,
            }
            saturation_params[ch] = {
                "lam": float(posterior["saturation_lam"].mean(dim=["chain", "draw"]).values[i])
                if "saturation_lam" in posterior
                else None,
            }
    except Exception as exc:
        logger.warning("Could not extract adstock/saturation params: %s", exc)

    # MCMC diagnostics (summary)
    diagnostics = {}
    try:
        summary_df = az.summary(mmm.fit_result)
        diagnostics["rhat_max"] = float(summary_df["r_hat"].max())
        diagnostics["ess_bulk_min"] = float(summary_df["ess_bulk"].min())
        diagnostics["divergences"] = int(
            mmm.fit_result.sample_stats["diverging"].sum().values
        )
    except Exception as exc:
        logger.warning("Could not extract diagnostics: %s", exc)

    return {
        "r2": r2,
        "contrib": contrib,
        "roi": roi,
        "engine": "pymc-marketing",
        "adstock_params": adstock_params,
        "saturation_params": saturation_params,
        "diagnostics": diagnostics,
    }


# ---------------------------------------------------------------------------
# Ridge fallback
# ---------------------------------------------------------------------------

def _fit_ridge_wide(
    df: pd.DataFrame,
    target_column: str,
    channel_columns: List[str],
) -> Dict[str, Any]:
    """Fit Ridge regression for wide-format data (channel spend as columns)."""
    from sklearn.linear_model import Ridge

    X = df[channel_columns].fillna(0.0).values
    y = df[target_column].fillna(0.0).values

    model = Ridge(alpha=1.0).fit(X, y)
    coef = model.coef_.tolist()
    denom = float(sum(abs(c) for c in coef)) or 1.0
    kpi_mean = float(df[target_column].mean())

    contrib = [
        {"channel": ch, "beta": float(b), "mean_share": float(abs(b)) / denom}
        for ch, b in zip(channel_columns, coef)
    ]
    roi = [
        {
            "channel": ch,
            "roi": float(b * kpi_mean / (float(df[ch].mean()) + 1e-6)),
        }
        for ch, b in zip(channel_columns, coef)
    ]

    return {
        "r2": float(model.score(X, y)),
        "contrib": contrib,
        "roi": roi,
        "engine": "ridge-fallback",
    }


def _fit_ridge_tall(
    df: pd.DataFrame,
    target_column: str,
    channel_columns: List[str],
) -> Dict[str, Any]:
    """Fit Ridge regression for tall-format data (channel/campaign rows)."""
    from sklearn.linear_model import Ridge

    df["__feature"] = df["channel"].astype(str) + "|" + df["campaign"].astype(str)
    spend_wide = (
        df.pivot_table(
            index="date", columns="__feature", values="spend", aggfunc="sum", fill_value=0
        )
        .sort_index()
    )

    if target_column not in df.columns:
        raise ValueError(f"Column '{target_column}' missing from dataset")

    y_series = df.groupby("date")[target_column].sum().reindex(spend_wide.index).fillna(0.0)
    X = spend_wide.values
    y = y_series.values
    features = list(spend_wide.columns)

    model = Ridge(alpha=1.0).fit(X, y)
    coef = model.coef_.tolist()
    denom = float(sum(abs(c) for c in coef)) or 1.0
    kpi_mean = float(y_series.mean())

    feature_mean_spend = {f: float(spend_wide[f].mean()) for f in features}

    campaigns = []
    channel_summary_acc: Dict[str, Dict[str, float]] = {}
    for f, b in zip(features, coef):
        ch_name, camp_name = f.split("|", 1)
        mean_share = float(abs(b)) / denom
        roi_val = float(b * kpi_mean / (feature_mean_spend.get(f, 0.0) + 1e-6))
        mroas = roi_val
        elasticity = float(b) * (feature_mean_spend.get(f, 0.0)) / (kpi_mean + 1e-6)
        campaigns.append({
            "channel": ch_name,
            "campaign": camp_name,
            "feature": f,
            "beta": float(b),
            "mean_share": mean_share,
            "roi": roi_val,
            "mroas": mroas,
            "elasticity": elasticity,
            "mean_spend": feature_mean_spend.get(f, 0.0),
        })
        acc = channel_summary_acc.setdefault(
            ch_name, {"spend": 0.0, "roi": 0.0, "mroas": 0.0, "elasticity": 0.0}
        )
        acc["spend"] += feature_mean_spend.get(f, 0.0)
        acc["roi"] += max(roi_val, 0.0)
        acc["mroas"] += max(mroas, 0.0)
        acc["elasticity"] += elasticity

    channel_summary = [
        {
            "channel": ch,
            "spend": vals["spend"],
            "roi": vals["roi"],
            "mroas": vals["mroas"],
            "elasticity": vals["elasticity"],
        }
        for ch, vals in channel_summary_acc.items()
    ]

    # Legacy compat: contrib / roi by channel
    contrib = [
        {
            "channel": ch,
            "beta": 0.0,
            "mean_share": float(
                sum(c["mean_share"] for c in campaigns if c["channel"] == ch)
            ),
        }
        for ch in channel_summary_acc.keys()
    ]
    roi = [
        {
            "channel": ch,
            "roi": float(
                next((s["roi"] for s in channel_summary if s["channel"] == ch), 0.0)
            ),
        }
        for ch in channel_summary_acc.keys()
    ]

    return {
        "r2": float(model.score(X, y)),
        "contrib": contrib,
        "roi": roi,
        "engine": "ridge-fallback",
        "campaigns": campaigns,
        "channel_summary": channel_summary,
    }


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

def fit_model(
    df: pd.DataFrame,
    target_column: str,
    channel_columns: List[str],
    control_columns: Optional[List[str]] = None,
    date_column: str = "date",
    adstock_cfg: Optional[Dict[str, Any]] = None,
    saturation_cfg: Optional[Dict[str, Any]] = None,
    mcmc_cfg: Optional[Dict[str, Any]] = None,
    force_engine: Optional[str] = None,
    random_seed: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Fit an MMM model.

    Tries PyMC-Marketing Bayesian engine first; falls back to Ridge if
    pymc-marketing is not installed or if fitting fails.

    Parameters
    ----------
    df : DataFrame with at least date, target, and channel columns.
    target_column : name of KPI column.
    channel_columns : list of spend column names.
    control_columns : optional list of covariate column names.
    date_column : name of date column.
    adstock_cfg : adstock hyperparameters (l_max, alpha priors).
    saturation_cfg : saturation hyperparameters (lam priors).
    mcmc_cfg : MCMC sampling parameters (draws, tune, chains, target_accept).
    force_engine : "bayesian" or "ridge" to override auto-detection.

    Returns
    -------
    dict with keys: r2, contrib, roi, engine, and optionally
    campaigns, channel_summary, adstock_params, saturation_params, diagnostics.
    """
    control_columns = control_columns or []
    adstock_cfg = adstock_cfg or {"l_max": 8}
    saturation_cfg = saturation_cfg or {}
    mcmc_cfg = mcmc_cfg or {"draws": 1000, "tune": 1000, "chains": 4, "target_accept": 0.9}

    # Detect tall vs wide format
    tall_cols = {"channel", "campaign", "spend"}
    is_tall = tall_cols.issubset(set(df.columns))

    use_bayesian = (
        PYMC_AVAILABLE
        and force_engine != "ridge"
        and not is_tall  # Bayesian only supports wide format currently
    ) or force_engine == "bayesian"

    if use_bayesian and PYMC_AVAILABLE:
        try:
            logger.info("Fitting Bayesian MMM (pymc-marketing)…")
            return _fit_bayesian(
                df=df,
                date_column=date_column,
                target_column=target_column,
                channel_columns=channel_columns,
                control_columns=control_columns,
                adstock_cfg=adstock_cfg,
                saturation_cfg=saturation_cfg,
                mcmc_cfg=mcmc_cfg,
                random_seed=random_seed,
            )
        except Exception as exc:
            logger.warning("Bayesian fitting failed (%s); falling back to Ridge", exc)

    # Ridge fallback
    if is_tall:
        logger.info("Fitting Ridge model (tall format)…")
        return _fit_ridge_tall(df, target_column, channel_columns)
    else:
        logger.info("Fitting Ridge model (wide format)…")
        return _fit_ridge_wide(df, target_column, channel_columns)
