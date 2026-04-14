from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd

from app.mmm_version import CURRENT_MMM_ENGINE_VERSION


def _update_run_progress(
    *,
    run_id: str,
    runs_obj: Dict[str, Any],
    save_runs_fn: Callable[[], None],
    now_iso_fn: Callable[[], str],
    status: str | None = None,
    stage: str,
    progress_pct: int,
    detail: str | None = None,
) -> None:
    run = {**runs_obj.get(run_id, {})}
    if status is not None:
        run["status"] = status
    run["stage"] = stage
    run["progress_pct"] = max(0, min(100, int(progress_pct)))
    run["updated_at"] = now_iso_fn()
    if detail is not None:
        run["detail"] = detail
    runs_obj[run_id] = run
    save_runs_fn()


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
    dataset_info = datasets_obj.get(cfg.dataset_id)
    if not dataset_info:
        _update_run_progress(
            run_id=run_id,
            runs_obj=runs_obj,
            save_runs_fn=save_runs_fn,
            now_iso_fn=now_iso_fn,
            status="error",
            stage="Dataset unavailable",
            progress_pct=100,
            detail="Dataset not found",
        )
        return
    csv_path = dataset_info.get("path")
    path = Path(csv_path) if isinstance(csv_path, str) else csv_path
    df = pd.read_csv(path, parse_dates=["date"])
    if cfg.kpi not in df.columns:
        _update_run_progress(
            run_id=run_id,
            runs_obj=runs_obj,
            save_runs_fn=save_runs_fn,
            now_iso_fn=now_iso_fn,
            status="error",
            stage="Mapping failed",
            progress_pct=100,
            detail=f"Column '{cfg.kpi}' missing",
        )
        return
    is_tall = {"channel", "campaign", "spend"}.issubset(set(df.columns))
    if not is_tall:
        for channel in cfg.spend_channels:
            if channel not in df.columns:
                _update_run_progress(
                    run_id=run_id,
                    runs_obj=runs_obj,
                    save_runs_fn=save_runs_fn,
                    now_iso_fn=now_iso_fn,
                    status="error",
                    stage="Mapping failed",
                    progress_pct=100,
                    detail=f"Column '{channel}' missing",
                )
                return
        spend_totals = df[cfg.spend_channels].apply(pd.to_numeric, errors="coerce").fillna(0).sum()
        if float(spend_totals.sum()) <= 0:
            _update_run_progress(
                run_id=run_id,
                runs_obj=runs_obj,
                save_runs_fn=save_runs_fn,
                now_iso_fn=now_iso_fn,
                status="error",
                stage="Spend validation failed",
                progress_pct=100,
                detail="MMM run cannot start because all selected spend channels have zero spend in the dataset.",
            )
            return
    else:
        total_spend = float(pd.to_numeric(df["spend"], errors="coerce").fillna(0).sum())
        if total_spend <= 0:
            _update_run_progress(
                run_id=run_id,
                runs_obj=runs_obj,
                save_runs_fn=save_runs_fn,
                now_iso_fn=now_iso_fn,
                status="error",
                stage="Spend validation failed",
                progress_pct=100,
                detail="MMM run cannot start because the dataset has zero spend.",
            )
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
        _update_run_progress(
            run_id=run_id,
            runs_obj=runs_obj,
            save_runs_fn=save_runs_fn,
            now_iso_fn=now_iso_fn,
            status="running",
            stage="Fitting media response model",
            progress_pct=45,
        )
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
            "stage": "Finished",
            "progress_pct": 100,
            "r2": result["r2"],
            "contrib": result["contrib"],
            "roi": result["roi"],
            "engine": result.get("engine", "unknown"),
            "engine_version": result.get("engine_version", CURRENT_MMM_ENGINE_VERSION),
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
            "stage": "Run failed",
            "progress_pct": 100,
            "detail": str(exc),
            "config": runs_obj[run_id].get("config", {}),
            "kpi_mode": getattr(cfg, "kpi_mode", "conversions"),
            "updated_at": now_iso_fn(),
        }
        save_runs_fn()
