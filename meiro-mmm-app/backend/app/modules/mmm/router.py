import io
import json
import uuid
from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException

from app.modules.mmm.schemas import (
    BudgetScenarioCreateRequest,
    BuildFromPlatformRequest,
    ModelConfig,
    OptimizeRequest,
    ValidateMappingRequest,
)
from app.services_budget_recommendations import (
    build_budget_recommendations,
    create_budget_scenario,
    serialize_budget_scenario,
)
from app.services_budget_realization import list_budget_realization, record_budget_realization_snapshot


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    get_runs_obj: Callable[[], Dict[str, Any]],
    get_datasets_obj: Callable[[], Dict[str, Dict[str, Any]]],
    get_expenses_obj: Callable[[], Dict[str, Any]],
    get_settings_obj: Callable[[], Any],
    get_mmm_platform_dir_obj: Callable[[], Path],
    ensure_journeys_loaded_fn: Callable[..., Any],
    now_iso_fn: Callable[[], str],
    save_runs_fn: Callable[[], None],
    fit_model_fn: Callable[[str, ModelConfig], None],
    build_mmm_dataset_from_platform_fn: Callable[..., Any],
    validate_mapping_fn: Callable[..., Any],
) -> APIRouter:
    router = APIRouter(tags=["mmm"])

    def _ensure_mmm_enabled() -> None:
        if not getattr(get_settings_obj().feature_flags, "mmm_enabled", False):
            raise HTTPException(status_code=404, detail="mmm_enabled flag is off")

    def _load_run_and_dataset_rows(run_id: str) -> tuple[Dict[str, Any], list[dict[str, Any]]]:
        run = get_runs_obj().get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Model not found")
        dataset_id = run.get("dataset_id") or (run.get("config") or {}).get("dataset_id")
        if not dataset_id:
            raise HTTPException(status_code=400, detail="Model dataset is unavailable")
        dataset_info = get_datasets_obj().get(str(dataset_id))
        if not dataset_info:
            raise HTTPException(status_code=404, detail="Dataset not found")
        path = dataset_info.get("path")
        if path is None:
            raise HTTPException(status_code=404, detail="Dataset file not found")
        p = Path(path) if isinstance(path, str) else path
        if not p.exists():
            raise HTTPException(status_code=404, detail="Dataset file not found")
        rows = pd.read_csv(p).fillna(0).to_dict(orient="records")
        return run, rows

    @router.get("/api/mmm/platform-options")
    def get_mmm_platform_options():
        _ensure_mmm_enabled()
        channels = set()
        for exp in get_expenses_obj().values():
            if getattr(exp, "status", "active") == "deleted":
                continue
            ch = getattr(exp, "channel", None)
            if ch:
                channels.add(ch)
        return {"spend_channels": sorted(channels), "covariates": []}

    @router.post("/api/mmm/datasets/build-from-platform")
    def build_mmm_dataset_from_platform_endpoint(body: BuildFromPlatformRequest, db=Depends(get_db_dependency)):
        _ensure_mmm_enabled()
        journeys = ensure_journeys_loaded_fn(db)
        expenses_list = list(get_expenses_obj().values())
        try:
            df, coverage = build_mmm_dataset_from_platform_fn(
                journeys=journeys,
                expenses=expenses_list,
                date_start=body.date_start,
                date_end=body.date_end,
                kpi_target=body.kpi_target,
                spend_channels=body.spend_channels,
                covariates=body.covariates or [],
                currency=body.currency,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        kpi_col = "sales" if body.kpi_target == "sales" else "conversions"
        dataset_id = f"platform-mmm-{uuid.uuid4().hex[:12]}"
        dest = get_mmm_platform_dir_obj() / f"{dataset_id}.csv"
        df.to_csv(dest, index=False)
        metadata = {
            "period_start": body.date_start,
            "period_end": body.date_end,
            "kpi_target": body.kpi_target,
            "kpi_column": kpi_col,
            "spend_channels": body.spend_channels,
            "covariates": body.covariates or [],
            "currency": body.currency,
            "source": "platform",
        }
        if body.kpi_target == "attribution":
            if body.attribution_model:
                metadata["attribution_model"] = body.attribution_model
            if body.attribution_config_id:
                metadata["attribution_config_id"] = body.attribution_config_id
        datasets = get_datasets_obj()
        datasets[dataset_id] = {
            "path": dest,
            "type": "sales" if body.kpi_target == "sales" else "attribution",
            "source": "platform",
            "metadata": metadata,
        }
        return {
            "dataset_id": dataset_id,
            "columns": list(df.columns),
            "preview_rows": df.head(10).to_dict(orient="records"),
            "coverage": coverage,
            "metadata": metadata,
            "path": str(dest),
            "type": datasets[dataset_id]["type"],
        }

    @router.post("/api/mmm/datasets/{dataset_id}/validate-mapping")
    def validate_mapping_endpoint(dataset_id: str, body: ValidateMappingRequest):
        _ensure_mmm_enabled()
        dataset_info = get_datasets_obj().get(dataset_id)
        if not dataset_info:
            raise HTTPException(status_code=404, detail="Dataset not found")
        path = dataset_info.get("path")
        if path is None:
            raise HTTPException(status_code=404, detail="Dataset file not found")
        p = Path(path) if isinstance(path, str) else path
        if not p.exists():
            raise HTTPException(status_code=404, detail="Dataset file not found")
        df = pd.read_csv(p)
        errors, warnings, details = validate_mapping_fn(
            df,
            date_column=body.date_column,
            kpi=body.kpi,
            spend_channels=body.spend_channels,
            covariates=body.covariates,
        )
        from app.services_mmm_mapping import get_missingness_top_offenders

        details["missingness_top"] = get_missingness_top_offenders(details, top_n=5)
        return {"errors": errors, "warnings": warnings, "details": details, "valid": len(errors) == 0}

    @router.post("/api/models")
    def run_model(cfg: ModelConfig, tasks: BackgroundTasks):
        _ensure_mmm_enabled()
        datasets = get_datasets_obj()
        if cfg.dataset_id not in datasets:
            raise HTTPException(status_code=404, detail="dataset_id not found")
        settings = get_settings_obj()
        if cfg.frequency == "W" and settings.mmm.frequency != "W":
            cfg.frequency = settings.mmm.frequency
        kpi_mode = getattr(cfg, "kpi_mode", "conversions") or "conversions"
        run_id = f"mmm_{uuid.uuid4().hex[:12]}"
        now = now_iso_fn()
        config_dict = json.loads(cfg.model_dump_json())
        dataset_meta = (datasets.get(cfg.dataset_id) or {}).get("metadata") or {}
        runs = get_runs_obj()
        runs[run_id] = {
            "status": "queued",
            "config": config_dict,
            "kpi_mode": kpi_mode,
            "created_at": now,
            "updated_at": now,
            "dataset_id": cfg.dataset_id,
        }
        if dataset_meta.get("attribution_model"):
            runs[run_id]["attribution_model"] = dataset_meta["attribution_model"]
        if dataset_meta.get("attribution_config_id"):
            runs[run_id]["attribution_config_id"] = dataset_meta["attribution_config_id"]
        save_runs_fn()
        tasks.add_task(fit_model_fn, run_id, cfg)
        return {"run_id": run_id, "status": "queued"}

    @router.get("/api/models")
    def list_models():
        _ensure_mmm_enabled()
        items = []
        for run_id, run in get_runs_obj().items():
            config = run.get("config") or {}
            items.append(
                {
                    "run_id": run_id,
                    "status": run.get("status", "unknown"),
                    "created_at": run.get("created_at"),
                    "updated_at": run.get("updated_at"),
                    "dataset_id": run.get("dataset_id") or config.get("dataset_id"),
                    "kpi_mode": run.get("kpi_mode"),
                    "kpi": config.get("kpi"),
                    "n_channels": len(config.get("spend_channels") or []),
                    "n_covariates": len(config.get("covariates") or []),
                    "r2": run.get("r2"),
                    "engine": run.get("engine"),
                }
            )
        items.sort(key=lambda item: (item.get("created_at") or ""), reverse=True)
        return items

    @router.get("/api/models/compare")
    def compare_models():
        _ensure_mmm_enabled()
        runs = get_runs_obj()
        if not runs:
            return []
        comparison: Dict[str, Any] = {}
        for run in runs.values():
            if run.get("status") != "finished":
                continue
            kpi_mode = run.get("kpi_mode", "conversions")
            for roi_entry in run.get("roi", []):
                ch = roi_entry["channel"]
                comparison.setdefault(ch, {"channel": ch, "roi": {}, "contrib": {}})
                comparison[ch]["roi"][kpi_mode] = roi_entry["roi"]
            for contrib_entry in run.get("contrib", []):
                ch = contrib_entry["channel"]
                comparison.setdefault(ch, {"channel": ch, "roi": {}, "contrib": {}})
                comparison[ch]["contrib"][kpi_mode] = contrib_entry["mean_share"]
        return list(comparison.values())

    @router.get("/api/models/{run_id}")
    def get_model(run_id: str):
        _ensure_mmm_enabled()
        res = get_runs_obj().get(run_id)
        if not res:
            raise HTTPException(status_code=404, detail="run_id not found")
        return res

    @router.get("/api/models/{run_id}/contrib")
    def channel_contrib(run_id: str):
        _ensure_mmm_enabled()
        return get_runs_obj().get(run_id, {}).get("contrib", [])

    @router.get("/api/models/{run_id}/roi")
    def roi(run_id: str):
        _ensure_mmm_enabled()
        return get_runs_obj().get(run_id, {}).get("roi", [])

    @router.post("/api/models/{run_id}/what_if")
    def what_if_scenario(run_id: str, scenario: Dict[str, float] = Body(..., embed=False)):
        _ensure_mmm_enabled()
        run = get_runs_obj().get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Model not found")
        roi_map = {row["channel"]: row["roi"] for row in run.get("roi", [])}
        contrib_map = {row["channel"]: row["mean_share"] for row in run.get("contrib", [])}
        if not roi_map or not contrib_map:
            raise HTTPException(status_code=400, detail="ROI or contribution data not available")
        channels = sorted(roi_map.keys())
        baseline_per_channel: Dict[str, float] = {}
        scenario_per_channel: Dict[str, float] = {}
        baseline_total = 0.0
        scenario_total = 0.0
        for ch in channels:
            roi_val = float(roi_map.get(ch, 0.0))
            share = float(contrib_map.get(ch, 0.0))
            base = roi_val * share
            mult = float(scenario.get(ch, 1.0))
            new_val = base * mult
            baseline_per_channel[ch] = base
            scenario_per_channel[ch] = new_val
            baseline_total += base
            scenario_total += new_val
        uplift_abs = scenario_total - baseline_total
        uplift_pct = (uplift_abs / baseline_total * 100.0) if baseline_total != 0 else 0.0
        return {
            "baseline": {"total_kpi": baseline_total, "per_channel": baseline_per_channel},
            "scenario": {"total_kpi": scenario_total, "per_channel": scenario_per_channel, "multipliers": scenario},
            "lift": {"absolute": uplift_abs, "percent": uplift_pct},
        }

    @router.get("/api/models/{run_id}/summary/channel")
    def get_channel_summary(run_id: str):
        _ensure_mmm_enabled()
        res = get_runs_obj().get(run_id)
        if not res:
            raise HTTPException(status_code=404, detail="Model not found")
        return res.get("channel_summary", [])

    @router.get("/api/models/{run_id}/summary/campaign")
    def get_campaign_summary(run_id: str):
        _ensure_mmm_enabled()
        res = get_runs_obj().get(run_id)
        if not res:
            raise HTTPException(status_code=404, detail="Model not found")
        return res.get("campaigns", [])

    @router.get("/api/models/{run_id}/export.csv")
    def export_campaign_plan(run_id: str):
        _ensure_mmm_enabled()
        res = get_runs_obj().get(run_id)
        if not res:
            raise HTTPException(status_code=404, detail="Model not found")
        campaigns = res.get("campaigns", [])
        if not campaigns:
            return "", 200, {"Content-Type": "text/csv"}
        out = io.StringIO()
        out.write("channel,campaign,spend,optimal_spend,roi,expected_conversions\n")
        for row in campaigns:
            spend = float(row.get("mean_spend", 0.0))
            roi_val = float(row.get("roi", 0.0))
            out.write(f"{row.get('channel')},{row.get('campaign')},{spend:.4f},{spend:.4f},{roi_val:.6f},{spend * roi_val:.4f}\n")
        return out.getvalue(), 200, {"Content-Type": "text/csv"}

    @router.post("/api/models/{run_id}/optimize")
    def optimize_budget(run_id: str, scenario: Dict[str, float]):
        _ensure_mmm_enabled()
        run = get_runs_obj().get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Model not found")
        roi_map = {row["channel"]: row["roi"] for row in run.get("roi", [])}
        contrib_map = {row["channel"]: row["mean_share"] for row in run.get("contrib", [])}
        baseline = sum(roi_map.get(ch, 0) * contrib_map.get(ch, 0) for ch in roi_map)
        new_score = sum(roi_map.get(ch, 0) * contrib_map.get(ch, 0) * scenario.get(ch, 1.0) for ch in roi_map)
        uplift = ((new_score - baseline) / baseline * 100) if baseline != 0 else 0
        return {"uplift": uplift, "predicted_kpi": new_score, "baseline": baseline}

    @router.post("/api/models/{run_id}/optimize/auto")
    def optimize_auto(run_id: str, request: OptimizeRequest = OptimizeRequest()):
        _ensure_mmm_enabled()
        from scipy.optimize import minimize
        import numpy as np

        run = get_runs_obj().get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Model not found")
        roi_map = {row["channel"]: row["roi"] for row in run.get("roi", [])}
        contrib_map = {row["channel"]: row["mean_share"] for row in run.get("contrib", [])}
        if not roi_map or not contrib_map:
            raise HTTPException(status_code=400, detail="ROI or contribution data not available")

        channels = list(roi_map.keys())
        n = len(channels)
        roi_values = np.maximum(np.array([roi_map.get(ch, 0) for ch in channels]), 0.01)
        contrib_raw = np.array([contrib_map.get(ch, 0) for ch in channels])
        contrib_sum = contrib_raw.sum()
        contrib_values = contrib_raw / contrib_sum if contrib_sum > 0 else contrib_raw
        baseline_score = float(np.sum(roi_values * contrib_values))

        def objective(x: Any) -> float:
            return -(np.sum(roi_values * contrib_values * x) - baseline_score)

        constraints = ({"type": "eq", "fun": lambda x: np.sum(x) - (request.total_budget * n)},)
        bounds: list[tuple[float, float]] = []
        per_constraints = request.channel_constraints or {}
        for ch in channels:
            constraint = per_constraints.get(ch)
            if constraint and constraint.locked:
                bounds.append((1.0, 1.0))
            else:
                lo = constraint.min if constraint and constraint.min is not None else request.min_spend
                hi = constraint.max if constraint and constraint.max is not None else request.max_spend
                bounds.append((float(lo), float(hi)))
        try:
            x0 = np.ones(n) * request.total_budget
            result = minimize(
                objective,
                x0,
                method="SLSQP",
                bounds=bounds,
                constraints=constraints,
                options={"maxiter": 1000},
            )
            if not result.success:
                return {
                    "optimal_mix": {ch: float(request.total_budget) for ch in channels},
                    "predicted_kpi": baseline_score,
                    "baseline_kpi": baseline_score,
                    "uplift": 0.0,
                    "message": "At baseline",
                }
            optimal_mix = {ch: float(val) for ch, val in zip(channels, result.x)}
            predicted = float(-result.fun + baseline_score)
            uplift = ((predicted - baseline_score) / baseline_score * 100) if baseline_score > 0 else 0
            return {
                "optimal_mix": optimal_mix,
                "predicted_kpi": max(predicted, baseline_score),
                "baseline_kpi": baseline_score,
                "uplift": max(uplift, 0),
                "message": f"Uplift: {max(uplift, 0):.1f}%",
            }
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Optimization error: {exc}")

    @router.get("/api/models/{run_id}/budget/recommendations")
    def get_budget_recommendations(
        run_id: str,
        objective: str = "protect_efficiency",
        total_budget_change_pct: float = 0.0,
    ):
        _ensure_mmm_enabled()
        run, dataset_rows = _load_run_and_dataset_rows(run_id)
        return build_budget_recommendations(
            run_id=run_id,
            run=run,
            dataset_rows=dataset_rows,
            objective=objective,
            total_budget_change_pct=total_budget_change_pct,
        )

    @router.get("/api/models/{run_id}/budget/scenarios")
    def list_budget_scenarios(run_id: str, db=Depends(get_db_dependency)):
        _ensure_mmm_enabled()
        from app.models_config_dq import BudgetScenario

        rows = (
            db.query(BudgetScenario)
            .filter(BudgetScenario.run_id == run_id)
            .order_by(BudgetScenario.created_at.desc())
            .limit(20)
            .all()
        )
        return {"items": [serialize_budget_scenario(db, row) for row in rows], "total": len(rows)}

    @router.post("/api/models/{run_id}/budget/scenarios")
    def create_budget_scenario_endpoint(
        run_id: str,
        body: BudgetScenarioCreateRequest,
        db=Depends(get_db_dependency),
    ):
        _ensure_mmm_enabled()
        _run, _dataset_rows = _load_run_and_dataset_rows(run_id)
        scenario = create_budget_scenario(
            db,
            run_id=run_id,
            objective=body.objective,
            total_budget_change_pct=body.total_budget_change_pct,
            multipliers=body.multipliers,
            recommendations=body.recommendations,
            summary={
                "run_id": run_id,
                "objective": body.objective,
                "total_budget_change_pct": body.total_budget_change_pct,
            },
            created_by="ui",
        )
        return serialize_budget_scenario(db, scenario)

    @router.get("/api/models/{run_id}/budget/scenarios/{scenario_id}")
    def get_budget_scenario(run_id: str, scenario_id: str, db=Depends(get_db_dependency)):
        _ensure_mmm_enabled()
        from app.models_config_dq import BudgetScenario

        row = (
            db.query(BudgetScenario)
            .filter(BudgetScenario.id == scenario_id, BudgetScenario.run_id == run_id)
            .first()
        )
        if not row:
            raise HTTPException(status_code=404, detail="Budget scenario not found")
        return serialize_budget_scenario(db, row)

    @router.get("/api/models/{run_id}/budget/realization")
    def get_budget_realization(run_id: str, db=Depends(get_db_dependency)):
        _ensure_mmm_enabled()
        return list_budget_realization(db, run_id=run_id)

    @router.post("/api/models/{run_id}/budget/scenarios/{scenario_id}/realization")
    def create_budget_realization_snapshot_endpoint(run_id: str, scenario_id: str, db=Depends(get_db_dependency)):
        _ensure_mmm_enabled()
        try:
            return record_budget_realization_snapshot(db, run_id=run_id, scenario_id=scenario_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return router
