from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, UploadFile

from app.modules.data_sources.schemas import (
    DataSourceCreatePayload,
    DataSourceRotateCredentialsPayload,
    DataSourceTestPayload,
    DataSourceUpdatePayload,
)
from app.services_data_sources_readiness import build_data_sources_readiness
from app.services_import_health import IMPORT_SOURCES, build_import_health


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    list_data_sources_fn: Callable[..., Any],
    create_data_source_fn: Callable[..., Any],
    update_data_source_fn: Callable[..., Any],
    test_data_source_payload_fn: Callable[..., Any],
    test_saved_data_source_fn: Callable[..., Any],
    disable_data_source_fn: Callable[..., Any],
    delete_data_source_fn: Callable[..., Any],
    rotate_data_source_credentials_fn: Callable[..., Any],
    get_data_dir_obj: Callable[[], Path],
    get_sample_dir_obj: Callable[[], Path],
    get_datasets_obj: Callable[[], Dict[str, Any]],
    get_expenses_obj: Callable[[], Dict[str, Any]],
    get_import_sync_state_obj: Callable[[], Dict[str, Any]],
    get_sync_in_progress_obj: Callable[[], set[str]],
    now_iso_fn: Callable[[], str],
    fetch_meta_fn: Callable[..., Dict[str, Any]],
    fetch_google_fn: Callable[..., Dict[str, Any]],
    fetch_linkedin_fn: Callable[..., Dict[str, Any]],
    get_access_token_for_provider_fn: Callable[..., Optional[str]],
    get_ds_config_effective_fn: Callable[[str, str], Any],
    build_smart_suggestions_fn: Callable[..., Dict[str, Any]],
    get_meiro_readiness_fn: Callable[[], Dict[str, Any]],
    get_journeys_fn: Callable[[Any], Any],
) -> APIRouter:
    router = APIRouter(tags=["data_sources"])

    def get_datasource_user(
        x_user_id: Optional[str] = Header(None, alias="X-User-Id"),
        x_user_role: Optional[str] = Header(None, alias="X-User-Role"),
    ):
        user_id = x_user_id or "system"
        can_edit = (x_user_role or "").strip().lower() in ("admin", "editor")
        return user_id, can_edit

    @router.get("/api/data-sources")
    def api_list_data_sources(
        category: Optional[str] = Query(None, description="warehouse|ad_platform|cdp"),
        workspace_id: str = Query("default"),
        db=Depends(get_db_dependency),
    ):
        return list_data_sources_fn(db, workspace_id=workspace_id, category=category)

    @router.post("/api/data-sources")
    def api_create_data_source(
        body: DataSourceCreatePayload,
        db=Depends(get_db_dependency),
        user_info: Tuple[str, bool] = Depends(get_datasource_user),
    ):
        _, can_edit = user_info
        if not can_edit:
            raise HTTPException(status_code=403, detail="Only admin or editor can create data sources")
        try:
            return create_data_source_fn(
                db,
                workspace_id=body.workspace_id,
                category=body.category,
                source_type=body.type,
                name=body.name,
                config_json=body.config_json or {},
                secrets=body.secrets or {},
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @router.put("/api/data-sources/{source_id}")
    def api_update_data_source(
        source_id: str,
        body: DataSourceUpdatePayload,
        db=Depends(get_db_dependency),
        user_info: Tuple[str, bool] = Depends(get_datasource_user),
    ):
        _, can_edit = user_info
        if not can_edit:
            raise HTTPException(status_code=403, detail="Only admin or editor can update data sources")
        try:
            out = update_data_source_fn(
                db,
                source_id=source_id,
                name=body.name,
                status=body.status,
                config_json=body.config_json,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if not out:
            raise HTTPException(status_code=404, detail="Data source not found")
        return out

    @router.post("/api/data-sources/test")
    def api_test_data_source(body: DataSourceTestPayload, db=Depends(get_db_dependency)):
        try:
            return test_data_source_payload_fn(
                db,
                source_type=body.type,
                config_json=body.config_json or {},
                secrets=body.secrets or {},
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @router.post("/api/data-sources/{source_id}/test")
    def api_test_saved_data_source(source_id: str, db=Depends(get_db_dependency)):
        out = test_saved_data_source_fn(db, source_id=source_id)
        if not out:
            raise HTTPException(status_code=404, detail="Data source not found")
        return out

    @router.post("/api/data-sources/{source_id}/disable")
    def api_disable_data_source(
        source_id: str,
        db=Depends(get_db_dependency),
        user_info: Tuple[str, bool] = Depends(get_datasource_user),
    ):
        _, can_edit = user_info
        if not can_edit:
            raise HTTPException(status_code=403, detail="Only admin or editor can disable data sources")
        if not disable_data_source_fn(db, source_id):
            raise HTTPException(status_code=404, detail="Data source not found")
        return {"ok": True}

    @router.delete("/api/data-sources/{source_id}")
    def api_delete_data_source(
        source_id: str,
        db=Depends(get_db_dependency),
        user_info: Tuple[str, bool] = Depends(get_datasource_user),
    ):
        _, can_edit = user_info
        if not can_edit:
            raise HTTPException(status_code=403, detail="Only admin or editor can delete data sources")
        if not delete_data_source_fn(db, source_id):
            raise HTTPException(status_code=404, detail="Data source not found")
        return {"ok": True}

    @router.post("/api/data-sources/{source_id}/rotate-credentials")
    def api_rotate_data_source_credentials(
        source_id: str,
        body: DataSourceRotateCredentialsPayload,
        db=Depends(get_db_dependency),
        user_info: Tuple[str, bool] = Depends(get_datasource_user),
    ):
        _, can_edit = user_info
        if not can_edit:
            raise HTTPException(status_code=403, detail="Only admin or editor can rotate credentials")
        try:
            out = rotate_data_source_credentials_fn(db, source_id=source_id, secrets=body.secrets or {})
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if not out:
            raise HTTPException(status_code=404, detail="Data source not found")
        return out

    @router.get("/api/imports/health")
    def get_import_health():
        import_sync_state = get_import_sync_state_obj()
        sync_in_progress = get_sync_in_progress_obj()
        return build_import_health(import_sync_state, sync_in_progress)

    @router.get("/api/data-sources/readiness")
    def get_data_sources_readiness(
        workspace_id: str = Query("default"),
        db=Depends(get_db_dependency),
    ):
        data_sources_payload = list_data_sources_fn(db, workspace_id=workspace_id, category=None)
        data_sources = data_sources_payload.get("items", []) if isinstance(data_sources_payload, dict) else (data_sources_payload or [])
        import_health = get_import_health()
        meiro_readiness = get_meiro_readiness_fn()
        journeys = get_journeys_fn(db) or []
        return build_data_sources_readiness(
            data_sources=data_sources,
            import_health=import_health,
            meiro_readiness=meiro_readiness,
            journeys_loaded=len(journeys),
        )

    @router.post("/api/imports/sync/{source}")
    def trigger_sync(
        source: str,
        since: Optional[str] = Query(None),
        until: Optional[str] = Query(None),
        db=Depends(get_db_dependency),
    ):
        if source not in IMPORT_SOURCES:
            raise HTTPException(status_code=404, detail=f"Unknown source: {source}")
        sync_in_progress = get_sync_in_progress_obj()
        if source in sync_in_progress:
            raise HTTPException(status_code=409, detail="Sync already in progress for this source")
        today = pd.Timestamp.utcnow().date()
        if not since:
            since = (today - pd.Timedelta(days=30)).isoformat()
        if not until:
            until = today.isoformat()
        import_sync_state = get_import_sync_state_obj()
        sync_in_progress.add(source)
        import_sync_state.setdefault(source, {})["last_attempt_at"] = now_iso_fn()
        import_sync_state[source]["period_start"] = since
        import_sync_state[source]["period_end"] = until
        import_sync_state[source]["last_error"] = None
        import_sync_state[source]["action_hint"] = None
        try:
            if source == "meta_ads":
                ad_account_id = get_ds_config_effective_fn("meta", "ad_account_id") or "me"
                result = fetch_meta_fn(ad_account_id=ad_account_id, since=since, until=until)
            elif source == "google_ads":
                result = fetch_google_fn(segments_date_from=since, segments_date_to=until)
            else:
                result = fetch_linkedin_fn(since=since, until=until)
            rows = result.get("rows", 0)
            import_sync_state[source]["last_success_at"] = now_iso_fn()
            import_sync_state[source]["status"] = "Healthy"
            import_sync_state[source]["records_imported"] = rows
            platform_total = 0.0
            csv_name = {"meta_ads": "meta_ads.csv", "google_ads": "google_ads.csv", "linkedin_ads": "linkedin_ads.csv"}.get(source)
            if csv_name and get_data_dir_obj().joinpath(csv_name).exists():
                try:
                    df = pd.read_csv(get_data_dir_obj() / csv_name)
                    platform_total = float(df["spend"].sum()) if "spend" in df.columns else 0.0
                except Exception:
                    pass
            import_sync_state[source]["platform_total"] = platform_total
            return {"source": source, "status": "success", "rows": rows, "platform_total": platform_total}
        except HTTPException:
            raise
        except Exception as exc:
            import_sync_state[source]["status"] = "Broken"
            import_sync_state[source]["last_error"] = str(exc)
            import_sync_state[source]["action_hint"] = "Reconnect credentials or check connection."
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            sync_in_progress.discard(source)

    @router.get("/api/imports/reconciliation")
    def get_reconciliation(
        service_period_start: Optional[str] = Query(None),
        service_period_end: Optional[str] = Query(None),
    ):
        period_start = service_period_start or ""
        period_end = service_period_end or ""
        rows = []
        expenses = get_expenses_obj()
        for source in IMPORT_SOURCES:
            state = get_import_sync_state_obj().get(source, {})
            platform_total = state.get("platform_total") or 0.0
            app_total = 0.0
            for exp in expenses.values():
                if getattr(exp, "status", "active") == "deleted":
                    continue
                if getattr(exp, "source_name", None) != source:
                    continue
                if period_start and getattr(exp, "service_period_start", None) and exp.service_period_start < period_start:
                    continue
                if period_end and getattr(exp, "service_period_end", None) and exp.service_period_end > period_end:
                    continue
                app_total += exp.converted_amount if getattr(exp, "converted_amount", None) is not None else exp.amount
            delta = platform_total - app_total
            delta_pct = (delta / platform_total * 100.0) if platform_total else 0.0
            rec_status = "OK" if abs(delta_pct) <= 1.0 else "Warning" if abs(delta_pct) <= 5.0 else "Critical"
            rows.append(
                {
                    "source": source,
                    "platform_total": platform_total,
                    "app_normalized_total": app_total,
                    "delta": delta,
                    "delta_pct": delta_pct,
                    "status": rec_status,
                }
            )
        return {"period_start": period_start, "period_end": period_end, "rows": rows}

    @router.get("/api/imports/reconciliation/drilldown")
    def get_reconciliation_drilldown(
        source: str,
        service_period_start: Optional[str] = Query(None),
        service_period_end: Optional[str] = Query(None),
    ):
        if source not in IMPORT_SOURCES:
            raise HTTPException(status_code=404, detail="Unknown source")
        out = {"source": source, "missing_days": [], "missing_campaigns": []}
        path = get_data_dir_obj() / f"{source}.csv"
        if path.exists():
            try:
                df = pd.read_csv(path)
                if "date" in df.columns and service_period_start and service_period_end:
                    in_range = df[df["date"].astype(str).between(service_period_start, service_period_end)]
                    if not in_range.empty:
                        out["missing_days"] = in_range["date"].astype(str).unique().tolist()[:10]
                if "campaign" in df.columns:
                    out["missing_campaigns"] = df["campaign"].dropna().unique().tolist()[:20]
            except Exception:
                pass
        return out

    @router.post("/api/datasets/upload")
    async def upload_dataset(file: UploadFile = File(...), dataset_id: Optional[str] = None, type: str = "sales"):
        if not file.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")
        dataset_id = dataset_id or file.filename.replace(".csv", "")
        dest = get_sample_dir_obj() / f"{dataset_id}.csv"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(await file.read())
        get_datasets_obj()[dataset_id] = {"path": dest, "type": type}
        df = pd.read_csv(dest).head(5)
        return {"dataset_id": dataset_id, "columns": list(df.columns), "preview_rows": df.to_dict(orient="records"), "path": str(dest), "type": type}

    @router.get("/api/datasets")
    def list_datasets():
        return [
            {"dataset_id": k, "path": str(v.get("path", "")), "type": v.get("type", "sales"), "source": v.get("source", "upload"), "metadata": v.get("metadata")}
            for k, v in get_datasets_obj().items()
        ]

    @router.get("/api/datasets/{dataset_id}")
    def get_dataset(dataset_id: str, preview_only: bool = True):
        dataset_info = get_datasets_obj().get(dataset_id)
        if not dataset_info:
            raise HTTPException(status_code=404, detail="Dataset not found")
        path = dataset_info.get("path")
        if path is None:
            return {
                "dataset_id": dataset_id,
                "columns": [],
                "preview_rows": [],
                "type": dataset_info.get("type", "sales"),
                "metadata": dataset_info.get("metadata"),
                "available": False,
                "detail": "Dataset file is not available in this runtime.",
            }
        p = Path(path) if isinstance(path, str) else path
        if not p.exists():
            return {
                "dataset_id": dataset_id,
                "columns": [],
                "preview_rows": [],
                "type": dataset_info.get("type", "sales"),
                "metadata": dataset_info.get("metadata"),
                "available": False,
                "detail": "Dataset file is not available in this runtime.",
            }
        df = pd.read_csv(p).head(5) if preview_only else pd.read_csv(p)
        out = {"dataset_id": dataset_id, "columns": list(df.columns), "preview_rows": df.to_dict(orient="records"), "type": dataset_info.get("type", "sales"), "available": True}
        if dataset_info.get("metadata"):
            out["metadata"] = dataset_info["metadata"]
        return out

    @router.get("/api/datasets/{dataset_id}/validate")
    def validate_dataset(dataset_id: str, kpi_target: Optional[str] = Query(None, description="sales | attribution for KPI suggestion bias")):
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
        columns = list(df.columns)
        n_rows = len(df)
        col_info = [{"name": col, "dtype": str(df[col].dtype), "missing": int(df[col].isna().sum()), "unique": int(df[col].nunique()), "sample_values": df[col].dropna().head(3).tolist()} for col in columns]
        suggestions = build_smart_suggestions_fn(df, kpi_target=kpi_target)
        date_column = suggestions.get("date_column")
        date_range = None
        if date_column and date_column in df.columns:
            try:
                parsed = pd.to_datetime(df[date_column], errors="coerce")
                date_range = {"min": str(parsed.min().date()), "max": str(parsed.max().date()), "n_periods": int(parsed.nunique())}
            except Exception:
                pass
        suggestions["spend_channels"] = suggestions.get("spend_channels") or []
        suggestions["kpi_columns"] = suggestions.get("kpi_columns") or []
        is_tall = {"channel", "campaign", "spend"}.issubset(set(columns))
        warnings = []
        if n_rows < 20:
            warnings.append(f"Only {n_rows} rows.")
        if date_column and date_range and date_range["n_periods"] < 20:
            warnings.append(f"Only {date_range['n_periods']} unique dates.")
        for ci in col_info:
            if ci["missing"] > 0 and n_rows and ci["missing"] / n_rows > 0.1:
                warnings.append(f"Column '{ci['name']}' has {ci['missing']/n_rows*100:.0f}% missing values.")
        return {"dataset_id": dataset_id, "n_rows": n_rows, "n_columns": len(columns), "columns": col_info, "date_column": date_column, "date_range": date_range, "format": "tall" if is_tall else "wide", "suggestions": suggestions, "warnings": warnings}

    return router
