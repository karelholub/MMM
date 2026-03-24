from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd


def fit_model(
    *,
    run_id: str,
    cfg: Any,
    runs_obj: Dict[str, Any],
    datasets_obj: Dict[str, Dict[str, Any]],
    now_iso_fn: Callable[[], str],
    save_runs_fn: Callable[[], None],
    mmm_fit_model_fn: Callable[..., Dict[str, Any]],
) -> None:
    now_ts = now_iso_fn()
    dataset_info = datasets_obj.get(cfg.dataset_id)
    if not dataset_info:
        runs_obj[run_id] = {**runs_obj.get(run_id, {}), "status": "error", "detail": "Dataset not found", "updated_at": now_ts}
        save_runs_fn()
        return
    csv_path = dataset_info.get("path")
    path = Path(csv_path) if isinstance(csv_path, str) else csv_path
    df = pd.read_csv(path, parse_dates=["date"])
    if cfg.kpi not in df.columns:
        runs_obj[run_id] = {**runs_obj.get(run_id, {}), "status": "error", "detail": f"Column '{cfg.kpi}' missing", "updated_at": now_ts}
        save_runs_fn()
        return
    is_tall = {"channel", "campaign", "spend"}.issubset(set(df.columns))
    if not is_tall:
        for channel in cfg.spend_channels:
            if channel not in df.columns:
                runs_obj[run_id] = {**runs_obj.get(run_id, {}), "status": "error", "detail": f"Column '{channel}' missing", "updated_at": now_ts}
                save_runs_fn()
                return
    priors = cfg.priors or {}
    adstock_cfg = {
        "l_max": 8,
        "alpha_mean": priors.get("adstock", {}).get("alpha_mean", 0.5),
        "alpha_sd": priors.get("adstock", {}).get("alpha_sd", 0.2),
    }
    saturation_cfg = {
        "lam_mean": priors.get("saturation", {}).get("lam_mean", 0.001),
        "lam_sd": priors.get("saturation", {}).get("lam_sd", 0.0005),
    }
    mcmc_cfg = cfg.mcmc or {"draws": 1000, "tune": 1000, "chains": 4, "target_accept": 0.9}
    use_adstock = getattr(cfg, "use_adstock", True)
    use_saturation = getattr(cfg, "use_saturation", True)
    force_engine = "ridge" if (not use_adstock and not use_saturation) else None
    random_seed = getattr(cfg, "random_seed", None)
    try:
        runs_obj[run_id]["status"] = "running"
        runs_obj[run_id]["updated_at"] = now_iso_fn()
        save_runs_fn()
        result = mmm_fit_model_fn(
            df=df,
            target_column=cfg.kpi,
            channel_columns=cfg.spend_channels,
            control_columns=cfg.covariates or [],
            date_column="date",
            adstock_cfg=adstock_cfg,
            saturation_cfg=saturation_cfg,
            mcmc_cfg=mcmc_cfg,
            force_engine=force_engine,
            random_seed=random_seed,
        )
        runs_obj[run_id] = {
            **runs_obj[run_id],
            "status": "finished",
            "r2": result["r2"],
            "contrib": result["contrib"],
            "roi": result["roi"],
            "engine": result.get("engine", "unknown"),
            "updated_at": now_iso_fn(),
        }
        for key in ("campaigns", "channel_summary", "adstock_params", "saturation_params", "diagnostics"):
            if key in result:
                runs_obj[run_id][key] = result[key]
        save_runs_fn()
    except Exception as exc:
        runs_obj[run_id] = {
            **runs_obj.get(run_id, {}),
            "status": "error",
            "detail": str(exc),
            "config": runs_obj[run_id].get("config", {}),
            "kpi_mode": getattr(cfg, "kpi_mode", "conversions"),
            "updated_at": now_iso_fn(),
        }
        save_runs_fn()
