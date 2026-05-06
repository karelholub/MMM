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
from app.services_activation_measurement import build_activation_feedback_recommendations
from app.services_budget_recommendations import (
    build_budget_recommendations,
    create_budget_scenario,
    serialize_budget_scenario,
)
from app.services_budget_realization import list_budget_realization, record_budget_realization_snapshot
from app.services_mmm_quality import evaluate_mmm_run_quality
from app.services_segments import list_membership_profile_ids_for_external_segment
from app.services_walled_garden import (
    is_walled_garden,
    load_ads_delivery_rows,
    normalize_channel,
    source_channel_from_synthetic_column,
    synthetic_column,
)
from app.utils.meiro_config import expense_matches_target_site_scope, site_scope_is_strict


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    get_runs_obj: Callable[[], Dict[str, Any]],
    get_datasets_obj: Callable[[], Dict[str, Dict[str, Any]]],
    get_expenses_obj: Callable[[], Dict[str, Any]],
    get_settings_obj: Callable[[], Any],
    get_data_dir_obj: Callable[[], Path],
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

    def _scoped_expenses() -> list[Any]:
        expenses = list(get_expenses_obj().values())
        if not site_scope_is_strict():
            return expenses
        return [
            expense
            for expense in expenses
            if expense_matches_target_site_scope(expense, allow_unknown=True)
        ]

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
        response = {
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
        return response

    def _budget_activation_feedback(db: Any) -> Dict[str, Any]:
        try:
            journeys = ensure_journeys_loaded_fn(db)
            return build_activation_feedback_recommendations(journeys=journeys, limit=5)
        except Exception:
            return {
                "items": [],
                "total": 0,
                "limit": 5,
                "summary": {"ready": 0, "warning": 0, "setup": 0},
                "decision": {
                    "status": "unavailable",
                    "subtitle": "Activation feedback is unavailable for this budget run.",
                    "blockers": [],
                    "warnings": ["Activation feedback could not be loaded from the current journey source."],
                    "actions": [],
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

    def _channel_response_basis(run: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
        roi_map = {
            str(row.get("channel")): float(row.get("roi") or 0.0)
            for row in run.get("roi", [])
            if isinstance(row, dict) and row.get("channel")
        }
        contrib_map = {
            str(row.get("channel")): row
            for row in run.get("contrib", [])
            if isinstance(row, dict) and row.get("channel")
        }
        summary_map = {
            str(row.get("channel")): row
            for row in run.get("channel_summary", [])
            if isinstance(row, dict) and row.get("channel")
        }
        channels = sorted(set(roi_map) | set(contrib_map) | set(summary_map))
        basis: Dict[str, Dict[str, float]] = {}
        for ch in channels:
            roi = roi_map.get(ch, 0.0)
            summary = summary_map.get(ch) or {}
            contrib = contrib_map.get(ch) or {}
            spend = float(summary.get("spend") or 0.0)
            contribution_raw = contrib.get("mean_contribution")
            contribution = float(contribution_raw) if contribution_raw is not None else 0.0
            if contribution <= 0 and spend > 0:
                contribution = max(roi, 0.0) * spend
            if contribution <= 0:
                contribution = max(roi, 0.0) * max(float(contrib.get("mean_share") or 0.0), 0.0)
            basis[ch] = {"roi": roi, "spend": spend, "contribution": contribution}
        return basis

    def _dataset_metadata(dataset_id: Any) -> Dict[str, Any]:
        if not dataset_id:
            return {}
        dataset_info = get_datasets_obj().get(str(dataset_id)) or {}
        metadata = dataset_info.get("metadata") or {}
        return metadata if isinstance(metadata, dict) else {}

    def _journey_profile_id(journey: Dict[str, Any]) -> str:
        customer = journey.get("customer") if isinstance(journey.get("customer"), dict) else {}
        return str(
            customer.get("id")
            or journey.get("customer_id")
            or journey.get("profile_id")
            or journey.get("user_id")
            or ""
        ).strip()

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
        for exp in _scoped_expenses():
            status = exp.get("status", "active") if isinstance(exp, dict) else getattr(exp, "status", "active")
            if status == "deleted":
                continue
            ch = exp.get("channel") if isinstance(exp, dict) else getattr(exp, "channel", None)
            if ch:
                channels.add(ch)
        delivery_rows = load_ads_delivery_rows(
            get_data_dir_obj(),
            date_start="1970-01-01",
            date_end="2999-12-31",
        )
        delivery_channels = sorted({normalize_channel(row.get("channel")) for row in delivery_rows if row.get("channel")})
        channels.update(delivery_channels)
        return {
            "spend_channels": sorted(channels),
            "covariates": [],
            "walled_garden_channels": delivery_channels,
            "media_input_modes": [
                {
                    "id": "spend",
                    "label": "Spend response",
                    "description": "Model profit or conversions from spend while adding synthetic impressions as diagnostics.",
                },
                {
                    "id": "synthetic_impressions",
                    "label": "Synthetic impression response",
                    "description": "Model the media response from normalized walled-garden exposure pressure.",
                },
            ],
        }

    @router.post("/api/mmm/datasets/build-from-platform")
    def build_mmm_dataset_from_platform_endpoint(body: BuildFromPlatformRequest, db=Depends(get_db_dependency)):
        _ensure_mmm_enabled()
        journeys = ensure_journeys_loaded_fn(db)
        measurement_audience = dict(body.measurement_audience or {})
        measurement_profile_ids: set[str] = set()
        if measurement_audience:
            external_segment_id = str(
                measurement_audience.get("external_segment_id")
                or measurement_audience.get("id")
                or ""
            ).strip()
            measurement_profile_ids = set(list_membership_profile_ids_for_external_segment(db, external_segment_id))
            if not measurement_profile_ids:
                raise HTTPException(
                    status_code=400,
                    detail="Selected measurement audience has no observed profile membership. Use a membership-backed audience or refresh Meiro/Pipes profile state.",
                )
            scoped_journeys = [journey for journey in journeys if _journey_profile_id(journey) in measurement_profile_ids]
            if not scoped_journeys:
                raise HTTPException(
                    status_code=400,
                    detail="Selected measurement audience has membership, but no matching journeys in the selected MMM workspace. Import/replay journeys with profile membership before audience-scoped MMM.",
                )
            measurement_audience["materialization_status"] = "journey_rows_filtered"
            measurement_audience["profile_count"] = len(measurement_profile_ids)
            measurement_audience["journey_rows"] = len(scoped_journeys)
            journeys = scoped_journeys
        expenses_list = _scoped_expenses()
        media_input_mode = body.media_input_mode if body.media_input_mode in {"spend", "synthetic_impressions"} else "spend"
        delivery_rows = (
            load_ads_delivery_rows(get_data_dir_obj(), date_start=body.date_start, date_end=body.date_end)
            if body.include_synthetic_impressions
            else []
        )
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
                delivery_rows=delivery_rows,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        kpi_col = "sales" if body.kpi_target == "sales" else "conversions"
        modeled_media_channels = (
            [synthetic_column(ch) for ch in body.spend_channels if is_walled_garden(ch)]
            if media_input_mode == "synthetic_impressions"
            else body.spend_channels
        )
        if media_input_mode == "synthetic_impressions":
            total_synthetic = sum(float(df.get(ch, pd.Series(dtype=float)).sum() or 0.0) for ch in modeled_media_channels)
            if total_synthetic <= 0:
                raise HTTPException(
                    status_code=400,
                    detail="No synthetic impressions were found for the selected channels and date range. Import platform delivery metrics or use spend response mode.",
                )
        dataset_id = f"platform-mmm-{uuid.uuid4().hex[:12]}"
        dest = get_mmm_platform_dir_obj() / f"{dataset_id}.csv"
        df.to_csv(dest, index=False)
        metadata = {
            "period_start": body.date_start,
            "period_end": body.date_end,
            "kpi_target": body.kpi_target,
            "kpi_column": kpi_col,
            "spend_channels": modeled_media_channels,
            "source_spend_channels": body.spend_channels,
            "covariates": body.covariates or [],
            "currency": body.currency,
            "source": "platform",
            "source_detail": "platform_journeys_expenses",
            "media_input_mode": media_input_mode,
            "synthetic_impressions": {
                "enabled": body.include_synthetic_impressions,
                "columns": coverage.get("synthetic_impression_columns", []),
                "totals": coverage.get("synthetic_impression_totals", {}),
                "delivery": coverage.get("delivery", {}),
            },
        }
        if body.source_contract:
            metadata["source_contract"] = body.source_contract
        if measurement_audience:
            metadata["measurement_audience"] = measurement_audience
            source_contract = dict(metadata.get("source_contract") or {})
            source_contract["measurement_audience"] = measurement_audience
            source_contract["audience_scope"] = f"Measurement audience: {measurement_audience.get('name') or measurement_audience.get('id')}"
            metadata["source_contract"] = source_contract
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
        if dataset_meta.get("media_input_mode"):
            runs[run_id]["media_input_mode"] = dataset_meta["media_input_mode"]
        if dataset_meta.get("synthetic_impressions"):
            runs[run_id]["synthetic_impressions"] = dataset_meta["synthetic_impressions"]
        if dataset_meta.get("source_contract"):
            runs[run_id]["source_contract"] = dataset_meta["source_contract"]
        if dataset_meta.get("measurement_audience"):
            runs[run_id]["measurement_audience"] = dataset_meta["measurement_audience"]
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
                    "source_contract": run.get("source_contract"),
                    "measurement_audience": run.get("measurement_audience"),
                    "attribution_model": run.get("attribution_model"),
                    "attribution_config_id": run.get("attribution_config_id"),
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
        channel_basis = _channel_response_basis(run)
        if not channel_basis:
            raise HTTPException(status_code=400, detail="ROI or contribution data not available")
        channels = sorted(channel_basis.keys())
        baseline_per_channel: Dict[str, float] = {}
        scenario_per_channel: Dict[str, float] = {}
        baseline_total = 0.0
        scenario_total = 0.0
        for ch in channels:
            base = float(channel_basis[ch].get("contribution") or 0.0)
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

    @router.get("/api/models/{run_id}/walled-garden-impact")
    def get_walled_garden_impact(run_id: str, db=Depends(get_db_dependency)):
        _ensure_mmm_enabled()
        from app.models_config_dq import Experiment, ExperimentResult

        run = get_runs_obj().get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Model not found")
        dataset_id = run.get("dataset_id") or (run.get("config") or {}).get("dataset_id")
        metadata = _dataset_metadata(dataset_id)
        synthetic_meta = run.get("synthetic_impressions") or metadata.get("synthetic_impressions") or {}
        delivery = (synthetic_meta.get("delivery") or {}).get("channels") or {}
        totals = synthetic_meta.get("totals") or {}
        columns = synthetic_meta.get("columns") or []
        media_input_mode = run.get("media_input_mode") or metadata.get("media_input_mode") or "spend"
        source_channels = metadata.get("source_spend_channels") or (run.get("config") or {}).get("spend_channels") or []
        basis = _channel_response_basis(run)
        experiment_rows = (
            db.query(Experiment, ExperimentResult)
            .outerjoin(ExperimentResult, ExperimentResult.experiment_id == Experiment.id)
            .filter(Experiment.channel.in_([normalize_channel(ch) for ch in source_channels] + list(source_channels)))
            .all()
        )
        calibration_by_channel: Dict[str, list[Dict[str, Any]]] = {}
        for exp, result in experiment_rows:
            channel_key = normalize_channel(getattr(exp, "channel", None))
            calibration_by_channel.setdefault(channel_key, []).append(
                {
                    "experiment_id": exp.id,
                    "name": exp.name,
                    "status": exp.status,
                    "start_at": exp.start_at.isoformat() if exp.start_at else None,
                    "end_at": exp.end_at.isoformat() if exp.end_at else None,
                    "uplift_abs": getattr(result, "uplift_abs", None) if result else None,
                    "uplift_rel": getattr(result, "uplift_rel", None) if result else None,
                    "p_value": getattr(result, "p_value", None) if result else None,
                    "has_result": result is not None,
                }
            )

        rows = []
        for source in source_channels:
            source_channel = normalize_channel(source)
            synthetic_col = synthetic_column(source_channel)
            if not is_walled_garden(source_channel) and synthetic_col not in totals and source_channel not in delivery:
                continue
            modeled_channel = synthetic_col if media_input_mode == "synthetic_impressions" else source
            channel_basis = basis.get(modeled_channel) or basis.get(source_channel) or {}
            detail = delivery.get(source_channel) or {}
            synthetic_total = float(totals.get(synthetic_col) or detail.get("synthetic_impressions") or 0.0)
            spend = float(channel_basis.get("spend") or detail.get("spend") or 0.0)
            contribution = float(channel_basis.get("contribution") or 0.0)
            roi = float(channel_basis.get("roi") or 0.0)
            profit_per_1000_synthetic = contribution / synthetic_total * 1000.0 if synthetic_total > 0 else None
            calibrations = sorted(
                calibration_by_channel.get(source_channel, []),
                key=lambda item: str(item.get("end_at") or item.get("start_at") or ""),
                reverse=True,
            )
            completed_calibrations = [item for item in calibrations if item.get("has_result")]
            calibration_status = "calibrated" if completed_calibrations else "planned" if calibrations else "not_calibrated"
            saturation = "unknown"
            if spend > 0 and synthetic_total > 0:
                synthetic_per_spend = synthetic_total / spend
                if roi <= 0:
                    saturation = "inefficient"
                elif synthetic_per_spend > 500 and roi < 1:
                    saturation = "high"
                elif roi < 1.5:
                    saturation = "watch"
                else:
                    saturation = "room_to_scale"
            rows.append(
                {
                    "source_channel": source_channel,
                    "modeled_channel": modeled_channel,
                    "synthetic_column": synthetic_col,
                    "modeled_from": media_input_mode,
                    "spend": spend,
                    "impressions": float(detail.get("impressions") or 0.0),
                    "synthetic_impressions": synthetic_total,
                    "contribution": contribution,
                    "roi": roi,
                    "profit_per_1000_synthetic_impressions": profit_per_1000_synthetic,
                    "confidence": detail.get("confidence") or ("low" if synthetic_total <= 0 else "medium"),
                    "confidence_score": detail.get("confidence_score") or 0.0,
                    "calibration": {
                        "status": calibration_status,
                        "experiments": calibrations[:3],
                        "latest_result": completed_calibrations[0] if completed_calibrations else None,
                        "recommendation": (
                            "Use the latest experiment result to calibrate budget decisions."
                            if completed_calibrations
                            else "An experiment exists but has no result yet."
                            if calibrations
                            else "Create a geo or audience holdout to calibrate this platform before major budget moves."
                        ),
                    },
                    "saturation": saturation,
                    "method": detail.get("method") or "synthetic_impressions_v1",
                    "caveats": detail.get("caveats") or ([] if synthetic_total > 0 else ["No imported delivery metrics were available for this channel."]),
                }
            )
        if not rows and columns:
            for col in columns:
                source_channel = source_channel_from_synthetic_column(str(col))
                rows.append(
                    {
                        "source_channel": source_channel,
                        "modeled_channel": col if media_input_mode == "synthetic_impressions" else source_channel,
                        "synthetic_column": col,
                        "modeled_from": media_input_mode,
                        "spend": 0.0,
                        "impressions": 0.0,
                        "synthetic_impressions": float(totals.get(col) or 0.0),
                        "contribution": 0.0,
                        "roi": 0.0,
                        "profit_per_1000_synthetic_impressions": None,
                        "confidence": "low",
                        "confidence_score": 0.0,
                        "calibration": {
                            "status": "not_calibrated",
                            "experiments": [],
                            "latest_result": None,
                            "recommendation": "Create a geo or audience holdout to calibrate this platform before major budget moves.",
                        },
                        "saturation": "unknown",
                        "method": "synthetic_impressions_v1",
                        "caveats": ["Synthetic impression metadata exists, but this run has no matching source channel mapping."],
                    }
                )
        rows.sort(key=lambda row: float(row.get("contribution") or 0.0), reverse=True)
        available = any(float(row.get("synthetic_impressions") or 0.0) > 0 for row in rows)
        return {
            "run_id": run_id,
            "available": available,
            "media_input_mode": media_input_mode,
            "method": "synthetic_impressions_v1",
            "summary": {
                "channels": len(rows),
                "channels_with_signal": sum(1 for row in rows if float(row.get("synthetic_impressions") or 0.0) > 0),
                "total_synthetic_impressions": sum(float(row.get("synthetic_impressions") or 0.0) for row in rows),
                "modeled_directly": media_input_mode == "synthetic_impressions",
                "calibrated_channels": sum(1 for row in rows if (row.get("calibration") or {}).get("status") == "calibrated"),
            },
            "rows": rows,
            "explainability": [
                "Synthetic impressions normalize imported platform delivery into exposure pressure using impressions, reach/frequency when available, video engagement, and clicks.",
                "MMM contribution and ROI still come from the fitted model; synthetic impressions explain walled-garden delivery pressure behind those modeled outcomes.",
                "Use low-confidence rows directionally and calibrate them with geo holdouts, lift studies, or campaign pause/budget-shock evidence.",
            ],
        }

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
            spend = float(row.get("spend") or row.get("mean_spend") or 0.0)
            roi_val = float(row.get("roi", 0.0))
            expected = float(row.get("mean_contribution") or (spend * roi_val))
            out.write(f"{row.get('channel')},{row.get('campaign')},{spend:.4f},{spend:.4f},{roi_val:.6f},{expected:.4f}\n")
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
        channel_basis = _channel_response_basis(run)
        if not channel_basis:
            raise HTTPException(status_code=400, detail="ROI or contribution data not available")
        baseline = sum(row["contribution"] for row in channel_basis.values())
        new_score = sum(row["contribution"] * float(scenario.get(ch, 1.0)) for ch, row in channel_basis.items())
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
        channel_basis = _channel_response_basis(run)
        if not channel_basis:
            raise HTTPException(status_code=400, detail="ROI or contribution data not available")

        channels = list(channel_basis.keys())
        n = len(channels)
        roi_values = np.maximum(np.array([channel_basis[ch]["roi"] for ch in channels]), 0.0)
        spend_values = np.array([channel_basis[ch]["spend"] for ch in channels])
        contribution_values = np.array([channel_basis[ch]["contribution"] for ch in channels])
        if float(spend_values.sum()) <= 0:
            spend_values = np.ones(n)
        baseline_score = float(np.sum(contribution_values))
        baseline_budget = float(np.sum(spend_values))

        def objective(x: Any) -> float:
            return -float(np.sum(roi_values * spend_values * x))

        constraints = ({"type": "eq", "fun": lambda x: float(np.sum(spend_values * x) - (baseline_budget * request.total_budget))},)
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
            predicted = float(-result.fun)
            uplift = ((predicted - baseline_score) / baseline_score * 100) if baseline_score > 0 else 0
            return {
                "optimal_mix": optimal_mix,
                "predicted_kpi": predicted,
                "baseline_kpi": baseline_score,
                "uplift": uplift,
                "message": f"Uplift: {uplift:.1f}%",
            }
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Optimization error: {exc}")

    @router.get("/api/models/{run_id}/budget/recommendations")
    def get_budget_recommendations(
        run_id: str,
        objective: str = "protect_efficiency",
        total_budget_change_pct: float = 0.0,
        db=Depends(get_db_dependency),
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
            response = _budget_blocked_response(run_id, run, quality, total_budget_change_pct, objective)
            response["activation_feedback"] = _budget_activation_feedback(db)
            return response
        quality = _run_quality(run, dataset_available=True)
        if not quality.get("can_use_budget"):
            response = _budget_blocked_response(run_id, run, quality, total_budget_change_pct, objective)
            response["activation_feedback"] = _budget_activation_feedback(db)
            return response
        response = build_budget_recommendations(
            run_id=run_id,
            run=run,
            dataset_rows=dataset_rows,
            objective=objective,
            total_budget_change_pct=total_budget_change_pct,
        )
        response["activation_feedback"] = _budget_activation_feedback(db)
        return response

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
