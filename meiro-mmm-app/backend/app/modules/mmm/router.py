import io
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException
from sqlalchemy import func

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
from app.services_mmm_quality import evaluate_mmm_run_quality


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
    stale_run_after = timedelta(hours=6)

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

    def _dataset_available(dataset_id: Any) -> bool:
        if not dataset_id:
            return False
        dataset_info = get_datasets_obj().get(str(dataset_id))
        if not dataset_info:
            return False
        path = dataset_info.get("path")
        if path is None:
            return False
        p = Path(path) if isinstance(path, str) else path
        return p.exists()

    def _run_channel_summary_spend(run: Dict[str, Any]) -> float | None:
        rows = run.get("channel_summary")
        if not isinstance(rows, list):
            return None
        total = 0.0
        saw_spend = False
        for row in rows:
            if not isinstance(row, dict) or "spend" not in row:
                continue
            saw_spend = True
            try:
                total += float(row.get("spend") or 0.0)
            except (TypeError, ValueError):
                continue
        return total if saw_spend else None

    def _run_quality(run: Dict[str, Any], *, dataset_available: bool | None = None) -> Dict[str, Any]:
        config = run.get("config") or {}
        channels = config.get("spend_channels") or []
        return evaluate_mmm_run_quality(
            run,
            dataset_available=dataset_available,
            channels_modeled=len(channels) if isinstance(channels, list) else None,
            total_spend=_run_channel_summary_spend(run),
        )

    def _budget_blocked_action(quality: Dict[str, Any]) -> Dict[str, str]:
        label = str(quality.get("label") or "").lower()
        reason_text = " ".join(str(item) for item in quality.get("reasons") or []).lower()
        if "dataset" in reason_text and ("unavailable" in reason_text or "runtime" in reason_text or "preview" in reason_text):
            return {
                "id": "rebuild_mmm_dataset",
                "label": "Rebuild or reattach MMM dataset",
                "domain": "mmm",
                "target_page": "mmm",
            }
        if label == "refresh needed" or "current mmm calculation contract" in reason_text:
            return {
                "id": "rerun_mmm_same_setup",
                "label": "Re-run same setup",
                "domain": "mmm",
                "target_page": "mmm",
            }
        return {
            "id": "review_mmm_inputs",
            "label": "Review MMM inputs",
            "domain": "mmm",
            "target_page": "mmm",
        }

    def _budget_blocked_subtitle(quality: Dict[str, Any], action: Dict[str, str]) -> str:
        if action["id"] == "rerun_mmm_same_setup":
            return "This saved MMM readout needs to be refreshed before it can drive new budget decisions."
        if action["id"] == "rebuild_mmm_dataset":
            return "Saved MMM results can still be reviewed, but budget recommendations need the linked dataset preview."
        if str(quality.get("level") or "") == "pending":
            return "Budget recommendations become available after the MMM run finishes successfully."
        return "This MMM run is not safe for optimizer recommendations until the model inputs produce usable media signal."

    def _budget_blocked_response(run_id: str, run: Dict[str, Any], quality: Dict[str, Any], total_budget_change_pct: float, objective: str) -> Dict[str, Any]:
        roi_rows = run.get("roi") or []
        roi_values = [float(row.get("roi") or 0.0) for row in roi_rows if isinstance(row, dict)]
        action = _budget_blocked_action(quality)
        return {
            "run_id": run_id,
            "objective": objective,
            "recommendations": [],
            "decision": {
                "status": "blocked",
                "subtitle": _budget_blocked_subtitle(quality, action),
                "blockers": quality.get("reasons") or ["MMM run quality is not sufficient for budget recommendations."],
                "actions": [action],
            },
            "summary": {
                "total_budget_change_pct": total_budget_change_pct,
                "baseline_spend_total": 0,
                "channels_considered": len(roi_rows),
                "periods": 0,
                "weighted_roi": sum(roi_values) / len(roi_values) if roi_values else 0,
                "quality": quality,
            },
        }

    def _assert_run_can_use_budget(run: Dict[str, Any], *, dataset_available: bool | None = None) -> Dict[str, Any]:
        quality = _run_quality(run, dataset_available=dataset_available)
        if not quality.get("can_use_budget"):
            detail = "; ".join(quality.get("reasons") or ["MMM run is not safe for budget actions."])
            raise HTTPException(status_code=400, detail=detail)
        return quality

    def _parse_run_ts(value: Any) -> datetime | None:
        if not value:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    def _mark_stale_mmm_runs() -> None:
        runs = get_runs_obj()
        now = _parse_run_ts(now_iso_fn()) or datetime.now(timezone.utc)
        changed = False
        for run_id, run in runs.items():
            status = str((run or {}).get("status") or "").lower()
            if status not in {"queued", "running"}:
                continue
            heartbeat = _parse_run_ts((run or {}).get("updated_at")) or _parse_run_ts((run or {}).get("created_at"))
            if heartbeat is None or now - heartbeat <= stale_run_after:
                continue
            run["status"] = "stale"
            run["stage"] = "Stale"
            run["stale_from_status"] = status
            run["stale_reason"] = "run_heartbeat_expired"
            run["stale_at"] = now_iso_fn()
            run["detail"] = (
                f"MMM run was {status} but has not updated for more than "
                f"{int(stale_run_after.total_seconds() // 3600)} hours. The background job is no longer active."
            )
            run["updated_at"] = run["stale_at"]
            runs[run_id] = run
            changed = True
        if changed:
            save_runs_fn()

    @router.get("/api/mmm/platform-options")
    def get_mmm_platform_options():
        _ensure_mmm_enabled()
        channels = set()
        for exp in get_expenses_obj().values():
            status = exp.get("status", "active") if isinstance(exp, dict) else getattr(exp, "status", "active")
            if status == "deleted":
                continue
            ch = exp.get("channel") if isinstance(exp, dict) else getattr(exp, "channel", None)
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
            "stage": "Queued",
            "progress_pct": 5,
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
    def list_models(db=Depends(get_db_dependency)):
        _ensure_mmm_enabled()
        from app.models_config_dq import BudgetScenario

        _mark_stale_mmm_runs()
        scenario_counts: Dict[str, int] = {}
        latest_scenario_at: Dict[str, Any] = {}
        for run_id, count, latest in (
            db.query(
                BudgetScenario.run_id,
                func.count(BudgetScenario.id),
                func.max(BudgetScenario.created_at),
            )
            .group_by(BudgetScenario.run_id)
            .all()
        ):
            scenario_counts[str(run_id)] = int(count or 0)
            latest_scenario_at[str(run_id)] = latest.isoformat() if latest else None

        items = []
        for run_id, run in get_runs_obj().items():
            config = run.get("config") or {}
            dataset_id = run.get("dataset_id") or config.get("dataset_id")
            dataset_available = _dataset_available(dataset_id)
            quality = _run_quality(run, dataset_available=dataset_available)
            items.append(
                {
                    "run_id": run_id,
                    "status": run.get("status", "unknown"),
                    "created_at": run.get("created_at"),
                    "updated_at": run.get("updated_at"),
                    "dataset_id": dataset_id,
                    "dataset_available": dataset_available,
                    "config": config,
                    "kpi_mode": run.get("kpi_mode"),
                    "kpi": config.get("kpi"),
                    "n_channels": len(config.get("spend_channels") or []),
                    "n_covariates": len(config.get("covariates") or []),
                    "r2": run.get("r2"),
                    "engine": run.get("engine"),
                    "engine_version": run.get("engine_version"),
                    "stage": run.get("stage"),
                    "progress_pct": run.get("progress_pct"),
                    "detail": run.get("detail"),
                    "stale_from_status": run.get("stale_from_status"),
                    "stale_reason": run.get("stale_reason"),
                    "stale_at": run.get("stale_at"),
                    "scenario_count": scenario_counts.get(run_id, 0),
                    "latest_scenario_at": latest_scenario_at.get(run_id),
                    "quality": quality,
                }
            )

        def list_priority(item: Dict[str, Any]) -> int:
            status = item.get("status")
            quality_level = ((item.get("quality") or {}).get("level") or "").lower()
            if status == "finished" and quality_level == "ready" and item.get("dataset_available") is not False:
                return 0
            if status == "finished" and quality_level == "directional":
                return 1
            if status in {"queued", "running"}:
                return 2
            if status == "finished":
                return 3
            if status == "stale":
                return 4
            if status == "error":
                return 5
            return 5

        def list_recency(item: Dict[str, Any]) -> float:
            parsed = _parse_run_ts(item.get("updated_at") or item.get("created_at"))
            return parsed.timestamp() if parsed else 0.0

        items.sort(key=lambda item: (list_priority(item), -list_recency(item)))
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
    def get_model(run_id: str, db=Depends(get_db_dependency)):
        _ensure_mmm_enabled()
        from app.models_config_dq import BudgetScenario

        _mark_stale_mmm_runs()
        res = get_runs_obj().get(run_id)
        if not res:
            raise HTTPException(status_code=404, detail="run_id not found")
        out = dict(res)
        dataset_id = out.get("dataset_id") or (out.get("config") or {}).get("dataset_id")
        out["dataset_available"] = _dataset_available(dataset_id)
        out["quality"] = _run_quality(out, dataset_available=out["dataset_available"])
        scenario_count, latest_scenario_at = (
            db.query(
                func.count(BudgetScenario.id),
                func.max(BudgetScenario.created_at),
            )
            .filter(BudgetScenario.run_id == run_id)
            .first()
        )
        out["scenario_count"] = int(scenario_count or 0)
        out["latest_scenario_at"] = latest_scenario_at.isoformat() if latest_scenario_at else None
        return out

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
        quality = _run_quality(run, dataset_available=_dataset_available(run.get("dataset_id") or (run.get("config") or {}).get("dataset_id")))
        if not quality.get("can_use_results"):
            detail = "; ".join(quality.get("reasons") or ["MMM run is not safe for scenario readouts."])
            raise HTTPException(status_code=400, detail=detail)
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
        _assert_run_can_use_budget(
            run,
            dataset_available=_dataset_available(run.get("dataset_id") or (run.get("config") or {}).get("dataset_id")),
        )
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
        _assert_run_can_use_budget(
            run,
            dataset_available=_dataset_available(run.get("dataset_id") or (run.get("config") or {}).get("dataset_id")),
        )
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
        try:
            run, dataset_rows = _load_run_and_dataset_rows(run_id)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            run = get_runs_obj().get(run_id)
            if not run:
                raise
            quality = _run_quality(run, dataset_available=False)
            return _budget_blocked_response(run_id, run, quality, total_budget_change_pct, objective)
        quality = _run_quality(run, dataset_available=True)
        if not quality.get("can_use_budget"):
            return _budget_blocked_response(run_id, run, quality, total_budget_change_pct, objective)
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
        run, _dataset_rows = _load_run_and_dataset_rows(run_id)
        _assert_run_can_use_budget(run, dataset_available=True)
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
