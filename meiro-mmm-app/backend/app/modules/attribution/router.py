import json
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response

from app.modules.attribution.schemas import (
    ImportPreCheckResponse,
    JourneySourceActivatePayload,
    LoadSampleRequest,
)
from app.services_conversions import filter_journeys_by_quality
from app.services_meiro_quarantine import create_quarantine_run, get_quarantine_run, update_quarantine_records


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    attribution_models_obj: List[str],
    get_journeys_fn: Callable[[Any], List[Dict[str, Any]]],
    get_import_runs_fn: Callable[..., List[Dict[str, Any]]],
    get_import_run_fn: Callable[[str], Optional[Dict[str, Any]]],
    get_last_successful_run_fn: Callable[[], Optional[Dict[str, Any]]],
    build_journeys_summary_fn: Callable[..., Dict[str, Any]],
    build_journeys_preview_fn: Callable[..., Dict[str, Any]],
    get_kpi_config_fn: Callable[[], Any],
    get_journey_source_state_fn: Callable[[], Dict[str, Any]],
    normalize_journey_source_fn: Callable[[Optional[str]], Optional[str]],
    set_active_journey_source_fn: Callable[[str], None],
    journey_source_availability_fn: Callable[[], List[Dict[str, Any]]],
    latest_upload_file_obj: Any,
    validate_and_normalize_fn: Callable[[Any], Dict[str, Any]],
    persist_journeys_fn: Callable[..., None],
    refresh_journey_aggregates_fn: Callable[..., None],
    save_last_import_result_fn: Callable[[Dict[str, Any]], None],
    load_last_import_result_fn: Callable[[], Dict[str, Any]],
    append_import_run_fn: Callable[..., None],
    load_sample_journeys_fn: Callable[..., Dict[str, Any]],
    import_journeys_from_cdp_fn: Callable[..., Dict[str, Any]],
    from_cdp_request_factory: Callable[..., Any],
    run_attribution_fn: Callable[..., Dict[str, Any]],
    load_config_and_meta_fn: Callable[..., tuple[Any, Any]],
    apply_model_config_fn: Callable[..., List[Dict[str, Any]]],
    get_settings_obj: Callable[[], Any],
    get_attribution_results_obj: Callable[[], Dict[str, Any]],
    get_data_dir_obj: Callable[[], Any],
    get_mapping_fn: Callable[[], Dict[str, Any]],
    attribution_mapping_config_cls: Any,
    canonicalize_meiro_profiles_fn: Callable[..., Dict[str, Any]],
    get_pull_config_fn: Callable[[], Dict[str, Any]],
    get_default_config_id_fn: Callable[[], Optional[str]],
    get_model_config_fn: Callable[[Any, str], Any],
    journey_revenue_value_fn: Callable[..., float],
    analyze_paths_fn: Callable[..., Dict[str, Any]],
    compute_next_best_action_fn: Callable[..., Dict[str, Any]],
    has_any_campaign_fn: Callable[[List[Dict[str, Any]]], bool],
    filter_nba_recommendations_fn: Callable[..., tuple[Dict[str, Any], Any]],
    step_string_fn: Callable[..., str],
    get_latest_quality_for_scope_fn: Callable[..., Any],
    build_conversion_paths_analysis_from_daily_fn: Callable[..., Dict[str, Any]],
    build_conversion_path_details_from_daily_fn: Callable[..., Dict[str, Any]],
    path_archetypes_cache_obj: Dict[tuple, Dict[str, Any]],
    compute_path_archetypes_fn: Callable[..., Dict[str, Any]],
    compute_path_anomalies_fn: Callable[..., Any],
    run_attribution_campaign_fn: Callable[..., Dict[str, Any]],
    expenses_obj: Dict[str, Any],
    compute_channel_performance_fn: Callable[..., List[Dict[str, Any]]],
    derive_efficiency_fn: Callable[..., Dict[str, Any]],
    compute_campaign_uplift_fn: Callable[..., Dict[str, Dict[str, Any]]],
    compute_campaign_trends_fn: Callable[..., Dict[str, Any]],
) -> APIRouter:
    router = APIRouter(tags=["attribution"])

    def _results_store() -> Dict[str, Any]:
        return get_attribution_results_obj()

    def _apply_attribution_filters(journeys: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        settings = get_settings_obj()
        threshold = int(getattr(settings.attribution, "min_journey_quality_score", 0) or 0)
        return filter_journeys_by_quality(journeys, threshold)

    def _build_consistency_payload(db: Any, journeys: List[Dict[str, Any]]) -> tuple[Dict[str, Any] | None, List[str]]:
        try:
            from app.services_journey_readiness import build_journey_readiness
            from app.services_journey_settings import (
                build_journey_settings_impact_preview,
                ensure_active_journey_settings,
            )
            from app.utils.kpi_config import load_kpi_config

            active_settings = ensure_active_journey_settings(db, actor="system")
            active_preview = build_journey_settings_impact_preview(
                db,
                draft_settings_json=active_settings.settings_json or {},
            )
            readiness = build_journey_readiness(
                journeys=journeys,
                kpi_config=load_kpi_config(),
                get_import_runs_fn=get_import_runs_fn,
                active_settings=active_settings,
                active_settings_preview=active_preview,
            )
            warnings = [
                *readiness.get("blockers", []),
                *readiness.get("warnings", []),
            ]
            return readiness, warnings
        except Exception:
            return None, []

    @router.get("/api/attribution/models")
    def list_attribution_models():
        return {"models": attribution_models_obj}

    @router.get("/api/attribution/journeys")
    def get_journeys_summary(db=Depends(get_db_dependency)):
        journeys = get_journeys_fn(db)
        if not journeys:
            runs = get_import_runs_fn(limit=1)
            last_run = runs[0] if runs and runs[0].get("status") == "success" else None
            readiness, consistency_warnings = _build_consistency_payload(db, [])
            return {
                "loaded": False,
                "count": 0,
                "converted": 0,
                "channels": [],
                "last_import_at": last_run.get("at") if last_run else None,
                "last_import_source": last_run.get("source") if last_run else None,
                "data_freshness_hours": None,
                "system_state": "empty",
                "validation": {"error_count": 0, "warn_count": 0},
                "readiness": readiness,
                "consistency_warnings": consistency_warnings,
            }

        summary = build_journeys_summary_fn(
            journeys=journeys,
            kpi_config=get_kpi_config_fn(),
            get_import_runs_fn=get_import_runs_fn,
        )
        readiness, consistency_warnings = _build_consistency_payload(db, journeys)
        summary["readiness"] = readiness
        summary["consistency_warnings"] = consistency_warnings
        return summary

    @router.get("/api/attribution/journeys/source-state")
    def get_journeys_source_state():
        state = get_journey_source_state_fn()
        active = normalize_journey_source_fn(state.get("active_source"))
        runs = get_import_runs_fn(status="success", limit=50)
        last_success = normalize_journey_source_fn((runs[0] or {}).get("source")) if runs else None
        if not active:
            active = last_success
        return {
            "active_source": active,
            "last_success_source": last_success,
            "available_sources": journey_source_availability_fn(),
            "updated_at": state.get("updated_at"),
        }

    @router.post("/api/attribution/journeys/activate-source")
    def activate_journey_source(payload: JourneySourceActivatePayload, db=Depends(get_db_dependency)):
        source = normalize_journey_source_fn(payload.source)
        if source not in {"sample", "upload", "meiro"}:
            raise HTTPException(status_code=400, detail="Unsupported source")

        if source == "sample":
            result = load_sample_journeys_fn(LoadSampleRequest(import_note=payload.import_note), db)
            set_active_journey_source_fn("sample")
            return {"ok": True, "active_source": "sample", "result": result}

        if source == "meiro":
            result = import_journeys_from_cdp_fn(from_cdp_request_factory(import_note=payload.import_note), db)
            set_active_journey_source_fn("meiro")
            return {"ok": True, "active_source": "meiro", "result": result}

        if not latest_upload_file_obj.exists():
            raise HTTPException(status_code=404, detail="No uploaded JSON source available. Upload a file first.")
        try:
            data = json.loads(latest_upload_file_obj.read_text(encoding="utf-8"))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to read uploaded source: {exc}")

        result = validate_and_normalize_fn(data)
        valid = result["valid_journeys"]
        summary = result["import_summary"]
        persist_journeys_fn(db, valid, replace=True)
        refresh_journey_aggregates_fn(db)
        save_last_import_result_fn(result)
        errs = [i for i in result.get("validation_items", []) if i.get("severity") == "error"]
        warns = [i for i in result.get("validation_items", []) if i.get("severity") == "warning"]
        append_import_run_fn(
            "upload",
            len(valid),
            "success",
            total=summary.get("total", 0),
            valid=summary.get("valid", 0),
            invalid=summary.get("invalid", 0),
            converted=summary.get("converted", 0),
            channels_detected=summary.get("channels_detected"),
            validation_summary={"top_errors": errs[:10], "top_warnings": warns[:10]},
            config_snapshot={"schema_version": result.get("schema_version", "1.0"), "activated_from": "source_selector"},
            preview_rows=[
                {
                    "customer_id": j.get("customer", {}).get("id", "?"),
                    "touchpoints": len(j.get("touchpoints", [])),
                    "converted": bool(j.get("conversions")),
                }
                for j in valid[:20]
            ],
            import_note=payload.import_note,
        )
        set_active_journey_source_fn("upload")
        return {"ok": True, "active_source": "upload", "result": {"count": len(valid), "message": f"Loaded {len(valid)} valid journeys"}}

    @router.get("/api/attribution/journeys/preview")
    def get_journeys_preview(limit: int = Query(20, ge=1, le=100), db=Depends(get_db_dependency)):
        journeys = get_journeys_fn(db)
        if not journeys:
            return {"rows": [], "columns": [], "total": 0}
        return build_journeys_preview_fn(journeys=journeys, limit=limit)

    @router.get("/api/attribution/journeys/validation")
    def get_journeys_validation(db=Depends(get_db_dependency)):
        last = load_last_import_result_fn()
        items = last.get("validation_items", [])
        summary = last.get("import_summary", {})
        error_count = sum(1 for x in items if x.get("severity") == "error")
        warn_count = sum(1 for x in items if x.get("severity") == "warning")
        return {
            "error_count": error_count,
            "warn_count": warn_count,
            "validation_items": items,
            "import_summary": summary,
            "total": summary.get("total", 0),
            "top_errors": [x["message"] for x in items if x.get("severity") == "error"][:5],
            "top_warnings": [x["message"] for x in items if x.get("severity") == "warning"][:5],
        }

    @router.get("/api/attribution/journeys/import-result")
    def get_import_result():
        return load_last_import_result_fn()

    @router.get("/api/attribution/journeys/row-details")
    def get_row_details(valid_index: int = Query(..., ge=0)):
        last = load_last_import_result_fn()
        items = last.get("items_detail", [])
        for item in items:
            if item.get("valid") and item.get("validIndex") == valid_index:
                return {"original": item.get("original"), "normalized": item.get("normalized"), "journeyIndex": item.get("journeyIndex")}
        raise HTTPException(status_code=404, detail="Row not found")

    @router.get("/api/attribution/journeys/validation-report")
    def download_validation_report():
        last = load_last_import_result_fn()
        content = json.dumps(last, indent=2, default=str)
        return Response(
            content=content,
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=validation-report.json"},
        )

    @router.get("/api/attribution/import-log")
    def get_import_log(
        status: Optional[str] = Query(None),
        source: Optional[str] = Query(None),
        since: Optional[str] = Query(None),
        until: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=200),
    ):
        return {"runs": get_import_runs_fn(status=status, source=source, since=since, until=until, limit=limit)}

    @router.get("/api/attribution/import-log/{run_id}")
    def get_import_run_detail(run_id: str):
        run = get_import_run_fn(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Import run not found")
        out = dict(run)
        out["at"] = run.get("finished_at") or run.get("started_at")
        out["count"] = run.get("valid") if run.get("status") == "success" else 0
        return out

    @router.post("/api/attribution/meiro/quarantine/{run_id}/reprocess")
    def reprocess_meiro_quarantine_run(
        run_id: str,
        payload: Dict[str, Any] = Body(default={}),
        db=Depends(get_db_dependency),
    ):
        run = get_quarantine_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Quarantine run not found")

        records = run.get("records") or []
        requested_indices = payload.get("record_indices")
        if isinstance(requested_indices, list) and requested_indices:
            picked_indices = []
            for item in requested_indices:
                try:
                    idx = int(item)
                except Exception:
                    continue
                if 0 <= idx < len(records) and idx not in picked_indices:
                    picked_indices.append(idx)
        else:
            picked_indices = list(range(len(records)))

        originals: List[Dict[str, Any]] = []
        for idx in picked_indices:
            record = records[idx] if 0 <= idx < len(records) else None
            original = record.get("original") if isinstance(record, dict) else None
            if isinstance(original, dict):
                originals.append(original)

        if not originals:
            raise HTTPException(status_code=400, detail="No original quarantined records available for reprocessing.")

        mapping = attribution_mapping_config_cls(**(get_mapping_fn() or {}))
        pull_cfg = get_pull_config_fn()
        settings = get_settings_obj()
        result = canonicalize_meiro_profiles_fn(
            originals,
            mapping=mapping.model_dump(),
            revenue_config=(settings.revenue_config.model_dump() if hasattr(settings.revenue_config, "model_dump") else settings.revenue_config),
            dedup_config=pull_cfg,
        )
        journeys = result.get("valid_journeys") or []

        effective_config_id = payload.get("config_id") or get_default_config_id_fn()
        if effective_config_id:
            cfg = get_model_config_fn(db, effective_config_id)
            if cfg:
                journeys = apply_model_config_fn(journeys, cfg.config_json or {})

        retry_quarantine_records = result.get("quarantine_records") or []
        retry_cleaning_report = ((result.get("import_summary") or {}).get("cleaning_report") or {})
        retry_quarantine_run = None
        if retry_quarantine_records:
            retry_quarantine_run = create_quarantine_run(
                source="meiro_quarantine_reprocess",
                import_note=payload.get("import_note") or f"Reprocessed from quarantine run {run_id}",
                parser_version=result.get("schema_version"),
                summary=retry_cleaning_report,
                records=retry_quarantine_records,
            )

        persist_to_attribution = bool(payload.get("persist_to_attribution", True))
        replace_existing = bool(payload.get("replace_existing", False))
        existing_journeys = [] if replace_existing else get_journeys_fn(db)
        persisted_count = len(existing_journeys)
        if persist_to_attribution:
            merged_journeys = [*existing_journeys, *journeys]
            persist_journeys_fn(db, merged_journeys, replace=True)
            refresh_journey_aggregates_fn(db)
            persisted_count = len(merged_journeys)
            update_quarantine_records(
                run_id,
                record_indices=picked_indices,
                status="reprocessed",
                note=payload.get("import_note") or f"Reprocessed from quarantine run {run_id}",
                metadata={
                    "reprocessed_count": len(journeys),
                    "retry_quarantine_run_id": retry_quarantine_run.get("id") if retry_quarantine_run else None,
                    "persisted_count": persisted_count,
                    "replace_existing": replace_existing,
                },
            )
            append_import_run_fn(
                "meiro_quarantine_reprocess",
                len(journeys),
                "success",
                total=int((result.get("import_summary") or {}).get("total", len(originals)) or len(originals)),
                valid=len(journeys),
                invalid=len(retry_quarantine_records),
                converted=sum(1 for j in journeys if j.get("converted", True)),
                channels_detected=sorted({tp.get("channel", "unknown") for j in journeys for tp in j.get("touchpoints", [])}),
                validation_summary={
                    "cleaning_report": retry_cleaning_report,
                    "top_quarantine_reasons": retry_cleaning_report.get("top_unresolved_patterns") or [],
                    "quarantine_run_id": retry_quarantine_run.get("id") if retry_quarantine_run else None,
                    "source_quarantine_run_id": run_id,
                },
                config_snapshot={
                    "source_quarantine_run_id": run_id,
                    "selected_record_count": len(picked_indices),
                    "replace_existing": replace_existing,
                },
                preview_rows=[
                    {
                        "customer_id": j.get("customer_id") or ((j.get("customer") or {}).get("id")) or "?",
                        "touchpoints": len(j.get("touchpoints", [])),
                        "value": journey_revenue_value_fn(j),
                    }
                    for j in journeys[:20]
                ],
                import_note=payload.get("import_note") or f"Reprocessed from quarantine run {run_id}",
            )

        return {
            "source_quarantine_run_id": run_id,
            "selected_record_count": len(picked_indices),
            "reprocessed_count": len(journeys),
            "quarantine_count": len(retry_quarantine_records),
            "quarantine_run_id": retry_quarantine_run.get("id") if retry_quarantine_run else None,
            "persisted_to_attribution": persist_to_attribution,
            "replace_existing": replace_existing,
            "existing_count": len(existing_journeys),
            "persisted_count": persisted_count,
            "import_summary": result.get("import_summary") or {},
        }

    @router.get("/api/attribution/import-precheck")
    def import_precheck(source: str = Query(...), db=Depends(get_db_dependency)):
        current = get_journeys_fn(db)
        current_count = len(current or [])
        current_converted = sum(1 for j in (current or []) if j.get("converted", True))
        current_channels = sorted(set(tp.get("channel", "unknown") for j in (current or []) for tp in j.get("touchpoints", [])))
        last_success = get_last_successful_run_fn()
        if last_success and last_success.get("source") == source:
            incoming_count = last_success.get("valid", 0)
            incoming_converted = last_success.get("converted", 0)
            incoming_channels = last_success.get("channels_detected", [])
        else:
            incoming_count = 0
            incoming_converted = 0
            incoming_channels = []
        would_overwrite = current_count > 0
        drop_pct = None
        warning = None
        if would_overwrite and current_converted > 0 and incoming_converted < current_converted:
            drop_pct = ((current_converted - incoming_converted) / current_converted) * 100
            if drop_pct > 10:
                warning = f"Converted journeys would drop by {drop_pct:.0f}%. Verify your import source and mapping."
        return ImportPreCheckResponse(
            would_overwrite=would_overwrite,
            current_count=current_count,
            current_converted=current_converted,
            current_channels=current_channels,
            incoming_count=incoming_count,
            incoming_converted=incoming_converted,
            incoming_channels=incoming_channels,
            converted_drop_pct=drop_pct,
            warning=warning,
        )

    @router.post("/api/attribution/import-log/{run_id}/rerun")
    def import_rerun(run_id: str, db=Depends(get_db_dependency)):
        run = get_import_run_fn(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Import run not found")
        source = run.get("source", "")
        if source == "sample":
            return load_sample_journeys_fn(req=LoadSampleRequest(), db=db)
        if source in ("meiro_webhook", "meiro_pull", "meiro"):
            return import_journeys_from_cdp_fn(req=from_cdp_request_factory(), db=db)
        raise HTTPException(status_code=400, detail=f"Rerun not supported for source: {source}")

    @router.get("/api/attribution/field-mapping")
    def get_field_mapping():
        return {
            "touchpoint_attr": "touchpoints",
            "value_attr": "conversion_value",
            "id_attr": "customer_id",
            "channel_field": "channel",
            "timestamp_field": "timestamp",
        }

    @router.post("/api/attribution/journeys/upload")
    async def upload_journeys(file: UploadFile = File(...), import_note: Optional[str] = Form(None), db=Depends(get_db_dependency)):
        try:
            content = await file.read()
            data = json.loads(content)
            if not (isinstance(data, list) or (isinstance(data, dict) and "journeys" in data)):
                if not (isinstance(data, dict) and data.get("schema_version") == "2.0"):
                    raise ValueError("Expected JSON array of journeys or v2 envelope with 'journeys'")
        except (json.JSONDecodeError, ValueError) as exc:
            append_import_run_fn("upload", 0, "error", str(exc))
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")

        try:
            latest_upload_file_obj.parent.mkdir(parents=True, exist_ok=True)
            latest_upload_file_obj.write_text(json.dumps(data, ensure_ascii=True), encoding="utf-8")
        except Exception:
            pass

        result = validate_and_normalize_fn(data)
        valid = result["valid_journeys"]
        summary = result["import_summary"]
        persist_journeys_fn(db, valid, replace=True)
        refresh_journey_aggregates_fn(db)
        save_last_import_result_fn(result)
        errs = [i for i in result.get("validation_items", []) if i.get("severity") == "error"]
        warns = [i for i in result.get("validation_items", []) if i.get("severity") == "warning"]
        append_import_run_fn(
            "upload",
            len(valid),
            "success",
            total=summary.get("total", 0),
            valid=summary.get("valid", 0),
            invalid=summary.get("invalid", 0),
            converted=summary.get("converted", 0),
            channels_detected=summary.get("channels_detected"),
            validation_summary={"top_errors": errs[:10], "top_warnings": warns[:10]},
            config_snapshot={"schema_version": result.get("schema_version", "1.0")},
            preview_rows=[
                {
                    "customer_id": j.get("customer", {}).get("id", "?"),
                    "touchpoints": len(j.get("touchpoints", [])),
                    "converted": bool(j.get("conversions")),
                }
                for j in valid[:20]
            ],
            import_note=import_note,
        )
        set_active_journey_source_fn("upload")
        return {
            "count": len(valid),
            "message": f"Loaded {len(valid)} valid journeys",
            "import_summary": summary,
            "validation_items": result["validation_items"],
        }

    @router.post("/api/attribution/journeys/from-cdp")
    def import_journeys_from_cdp(req: Any = Body(default=None), db=Depends(get_db_dependency)):
        if req is None:
            req = from_cdp_request_factory()
        data_dir = get_data_dir_obj()
        cdp_json_path = data_dir / "meiro_cdp_profiles.json"
        cdp_path = data_dir / "meiro_cdp.csv"
        saved = get_mapping_fn()
        base = attribution_mapping_config_cls(
            touchpoint_attr=saved.get("touchpoint_attr", "touchpoints"),
            value_attr=saved.get("value_attr", "conversion_value"),
            id_attr=saved.get("id_attr", "customer_id"),
            channel_field=saved.get("channel_field", "channel"),
            timestamp_field=saved.get("timestamp_field", "timestamp"),
            source_field=saved.get("source_field", "source"),
            medium_field=saved.get("medium_field", "medium"),
            campaign_field=saved.get("campaign_field", "campaign"),
            currency_field=saved.get("currency_field", "currency"),
        )
        mapping = base
        if getattr(req, "mapping", None):
            mapping = attribution_mapping_config_cls(
                touchpoint_attr=req.mapping.touchpoint_attr or base.touchpoint_attr,
                value_attr=req.mapping.value_attr or base.value_attr,
                id_attr=req.mapping.id_attr or base.id_attr,
                channel_field=req.mapping.channel_field or base.channel_field,
                timestamp_field=req.mapping.timestamp_field or base.timestamp_field,
                source_field=req.mapping.source_field or base.source_field,
                medium_field=req.mapping.medium_field or base.medium_field,
                campaign_field=req.mapping.campaign_field or base.campaign_field,
                currency_field=req.mapping.currency_field or base.currency_field,
            )

        source_label = "meiro_webhook"
        if cdp_json_path.exists():
            with open(cdp_json_path) as fh:
                profiles = json.load(fh)
        elif cdp_path.exists():
            df = pd.read_csv(cdp_path)
            profiles = df.to_dict(orient="records")
            source_label = "meiro_pull"
        else:
            append_import_run_fn("meiro_webhook", 0, "error", error="No CDP data found. Fetch from Meiro CDP first.")
            raise HTTPException(status_code=404, detail="No CDP data found. Fetch from Meiro CDP first.")

        try:
            pull_cfg = get_pull_config_fn()
            settings = get_settings_obj()
            result = canonicalize_meiro_profiles_fn(
                profiles,
                mapping=mapping.model_dump(),
                revenue_config=(settings.revenue_config.model_dump() if hasattr(settings.revenue_config, "model_dump") else settings.revenue_config),
                dedup_config=pull_cfg,
            )
            journeys = result["valid_journeys"]
        except Exception as exc:
            append_import_run_fn(source_label, 0, "error", error=str(exc))
            raise

        effective_config_id = getattr(req, "config_id", None) or get_default_config_id_fn()
        if effective_config_id:
            cfg = get_model_config_fn(db, effective_config_id)
            if cfg:
                journeys = apply_model_config_fn(journeys, cfg.config_json or {})

        quarantine_records = result.get("quarantine_records") or []
        cleaning_report = ((result.get("import_summary") or {}).get("cleaning_report") or {})
        if quarantine_records:
            create_quarantine_run(
                source=source_label,
                import_note=getattr(req, "import_note", None),
                parser_version=result.get("schema_version"),
                summary=cleaning_report,
                records=quarantine_records,
            )

        persist_journeys_fn(db, journeys, replace=True)
        refresh_journey_aggregates_fn(db)
        converted = sum(1 for j in journeys if j.get("converted", True))
        ch_set = {tp.get("channel", "unknown") for j in journeys for tp in j.get("touchpoints", [])}
        summary = result.get("import_summary") or {}
        pull_cfg = get_pull_config_fn()
        append_import_run_fn(
            source_label,
            len(journeys),
            "success",
            total=int(summary.get("total", len(journeys)) or len(journeys)),
            valid=len(journeys),
            invalid=len(quarantine_records),
            converted=converted,
            channels_detected=sorted(ch_set),
            validation_summary={
                "cleaning_report": cleaning_report,
                "top_quarantine_reasons": cleaning_report.get("top_unresolved_patterns") or [],
            },
            config_snapshot={
                "mapping_preset": "saved",
                "schema_version": "1.0",
                "dedup_interval_minutes": pull_cfg.get("dedup_interval_minutes"),
                "dedup_mode": pull_cfg.get("dedup_mode"),
                "primary_dedup_key": pull_cfg.get("primary_dedup_key"),
                "fallback_dedup_keys": pull_cfg.get("fallback_dedup_keys"),
                "strict_ingest": pull_cfg.get("strict_ingest"),
                "timestamp_fallback_policy": pull_cfg.get("timestamp_fallback_policy"),
                "value_fallback_policy": pull_cfg.get("value_fallback_policy"),
                "currency_fallback_policy": pull_cfg.get("currency_fallback_policy"),
            },
            preview_rows=[
                {
                    "customer_id": j.get("customer_id", "?"),
                    "touchpoints": len(j.get("touchpoints", [])),
                    "value": journey_revenue_value_fn(j),
                }
                for j in journeys[:20]
            ],
            import_note=getattr(req, "import_note", None),
        )
        set_active_journey_source_fn("meiro")
        return {
            "count": len(journeys),
            "message": f"Parsed {len(journeys)} journeys from CDP data",
            "import_summary": summary,
            "quarantine_count": len(quarantine_records),
        }

    @router.post("/api/attribution/journeys/load-sample")
    def load_sample_journeys(req: LoadSampleRequest = Body(default=LoadSampleRequest()), db=Depends(get_db_dependency)):
        return load_sample_journeys_fn(req=req, db=db)

    @router.post("/api/attribution/run")
    def run_attribution_model(model: str = "linear", config_id: Optional[str] = None, db=Depends(get_db_dependency)):
        journeys = get_journeys_fn(db)
        if not journeys:
            raise HTTPException(status_code=400, detail="No journeys loaded. Upload, import, or persist data first.")
        if model not in attribution_models_obj:
            raise HTTPException(status_code=400, detail=f"Unknown model: {model}. Available: {attribution_models_obj}")
        resolved_cfg, meta = load_config_and_meta_fn(db, config_id)
        journeys_for_model = apply_model_config_fn(journeys, resolved_cfg.config_json or {}) if resolved_cfg else journeys
        journeys_for_model = _apply_attribution_filters(journeys_for_model)
        settings = get_settings_obj()
        kwargs: Dict[str, Any] = {}
        if model == "time_decay":
            kwargs["half_life_days"] = settings.attribution.time_decay_half_life_days
        elif model == "position_based":
            kwargs["first_pct"] = settings.attribution.position_first_pct
            kwargs["last_pct"] = settings.attribution.position_last_pct
        result = run_attribution_fn(journeys_for_model, model=model, **kwargs)
        if meta:
            result["config"] = meta
        _results_store()[model] = result
        return result

    @router.post("/api/attribution/run-all")
    def run_all_attribution_models(config_id: Optional[str] = None, db=Depends(get_db_dependency)):
        journeys = get_journeys_fn(db)
        if not journeys:
            raise HTTPException(status_code=400, detail="No journeys loaded. Upload, import, or persist data first.")
        resolved_cfg, meta = load_config_and_meta_fn(db, config_id)
        journeys_for_model = apply_model_config_fn(journeys, resolved_cfg.config_json or {}) if resolved_cfg else journeys
        journeys_for_model = _apply_attribution_filters(journeys_for_model)
        settings = get_settings_obj()
        results = []
        for model in attribution_models_obj:
            try:
                kwargs: Dict[str, Any] = {}
                if model == "time_decay":
                    kwargs["half_life_days"] = settings.attribution.time_decay_half_life_days
                elif model == "position_based":
                    kwargs["first_pct"] = settings.attribution.position_first_pct
                    kwargs["last_pct"] = settings.attribution.position_last_pct
                result = run_attribution_fn(journeys_for_model, model=model, **kwargs)
                if meta:
                    result["config"] = meta
                results.append(result)
                _results_store()[model] = result
            except Exception as exc:
                results.append({"model": model, "error": str(exc)})
        return {"results": results}

    @router.get("/api/attribution/results")
    def get_attribution_results():
        return _results_store()

    @router.get("/api/attribution/results/{model}")
    def get_attribution_result(model: str):
        result = _results_store().get(model)
        if not result:
            raise HTTPException(status_code=404, detail=f"No results for model '{model}'. Run it first.")
        return result

    def _attribution_weekly_series(
        journeys: List[Dict[str, Any]],
        model: str,
        date_start: str,
        date_end: str,
        config_id: Optional[str] = None,
        db=None,
    ) -> List[Dict[str, Any]]:
        week_freq = "W-MON"
        ds = pd.to_datetime(date_start, errors="coerce")
        de = pd.to_datetime(date_end, errors="coerce")
        if pd.isna(ds) or pd.isna(de) or ds > de:
            return []
        date_start_ts = ds.normalize().to_period(week_freq).start_time
        date_end_ts = de.normalize().to_period(week_freq).start_time
        week_range = pd.date_range(start=date_start_ts, end=date_end_ts, freq=week_freq)

        def _conversion_week(journey: Dict[str, Any]):
            last_ts = None
            for tp in journey.get("touchpoints") or []:
                ts = tp.get("timestamp")
                if not ts:
                    continue
                t = pd.to_datetime(ts, errors="coerce")
                if pd.notna(t):
                    last_ts = t if last_ts is None else max(last_ts, t)
            if last_ts is None:
                return None
            return last_ts.normalize().to_period(week_freq).start_time

        resolved_cfg, _meta = load_config_and_meta_fn(db, config_id) if db else (None, None)
        journeys_for_model = apply_model_config_fn(journeys, resolved_cfg.config_json or {}) if resolved_cfg else journeys
        journeys_for_model = _apply_attribution_filters(journeys_for_model)
        settings = get_settings_obj()
        kwargs: Dict[str, Any] = {}
        if model == "time_decay" and settings.attribution.time_decay_half_life_days:
            kwargs["half_life_days"] = settings.attribution.time_decay_half_life_days
        if model == "position_based":
            kwargs["first_pct"] = settings.attribution.position_first_pct
            kwargs["last_pct"] = settings.attribution.position_last_pct

        out = []
        for week in week_range:
            week_journeys = [j for j in journeys_for_model if j.get("converted", True) and _conversion_week(j) == week]
            if not week_journeys:
                out.append({"date": week.strftime("%Y-%m-%d"), "attributed_value": 0.0})
                continue
            try:
                res = run_attribution_fn(week_journeys, model=model, **kwargs)
                out.append({"date": week.strftime("%Y-%m-%d"), "attributed_value": float(res.get("total_value", 0) or 0)})
            except Exception:
                out.append({"date": week.strftime("%Y-%m-%d"), "attributed_value": 0.0})
        return out

    @router.get("/api/attribution/weekly")
    def get_attribution_weekly(
        model: str = Query(..., description="Attribution model id"),
        date_start: str = Query(..., description="ISO date start"),
        date_end: str = Query(..., description="ISO date end"),
        config_id: Optional[str] = Query(None),
        db=Depends(get_db_dependency),
    ):
        journeys = get_journeys_fn(db)
        series = _attribution_weekly_series(journeys, model, date_start, date_end, config_id, db)
        return {"model": model, "config_id": config_id, "series": series}

    @router.get("/api/attribution/paths")
    def get_path_analysis(
        config_id: Optional[str] = None,
        direct_mode: str = "include",
        path_scope: str = "converted",
        db=Depends(get_db_dependency),
    ):
        journeys = get_journeys_fn(db)
        if not journeys:
            raise HTTPException(status_code=400, detail="No journeys loaded.")
        resolved_cfg, meta = load_config_and_meta_fn(db, config_id)
        journeys_for_analysis = apply_model_config_fn(journeys, resolved_cfg.config_json or {}) if resolved_cfg else journeys

        direct_mode_normalized = (direct_mode or "include").lower()
        if direct_mode_normalized not in ("include", "exclude"):
            direct_mode_normalized = "include"
        if direct_mode_normalized == "exclude":
            filtered_journeys = []
            for journey in journeys_for_analysis:
                kept = [tp for tp in journey.get("touchpoints", []) if tp.get("channel", "").lower() != "direct"]
                if not kept:
                    continue
                j2 = dict(journey)
                j2["touchpoints"] = kept
                filtered_journeys.append(j2)
            journeys_for_analysis = filtered_journeys

        include_non_converted = (path_scope or "converted").lower() in ("all", "all_journeys", "include_non_converted")
        path_analysis = analyze_paths_fn(journeys_for_analysis, include_non_converted=include_non_converted)

        nba_channel_raw = compute_next_best_action_fn(journeys_for_analysis, level="channel")
        filtered_channel, _channel_stats = filter_nba_recommendations_fn(nba_channel_raw, get_settings_obj().nba)
        path_analysis["next_best_by_prefix"] = filtered_channel

        if has_any_campaign_fn(journeys_for_analysis):
            nba_campaign_raw = compute_next_best_action_fn(journeys_for_analysis, level="campaign")
            filtered_campaign, _campaign_stats = filter_nba_recommendations_fn(nba_campaign_raw, get_settings_obj().nba)
            path_analysis["next_best_by_prefix_campaign"] = filtered_campaign

        path_analysis["config"] = meta
        path_analysis["view_filters"] = {
            "direct_mode": direct_mode_normalized,
            "path_scope": "all" if include_non_converted else "converted",
        }
        path_analysis["nba_config"] = get_settings_obj().nba.model_dump()
        return path_analysis

    @router.get("/api/conversion-paths/analysis")
    def get_conversion_paths_analysis_aggregated(
        definition_id: Optional[str] = Query(None, description="Optional journey definition id"),
        date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
        date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
        direct_mode: str = Query("include", description="include|exclude"),
        path_scope: str = Query("converted", description="converted|all"),
        channel_group: Optional[str] = Query(None),
        campaign_id: Optional[str] = Query(None),
        device: Optional[str] = Query(None),
        country: Optional[str] = Query(None),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        d_from = None
        d_to = None
        try:
            if date_from:
                d_from = pd.to_datetime(date_from).date()
            if date_to:
                d_to = pd.to_datetime(date_to).date()
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
        if d_from and d_to and d_from > d_to:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")
        return build_conversion_paths_analysis_from_daily_fn(
            db,
            definition_id=definition_id,
            date_from=d_from,
            date_to=d_to,
            direct_mode=(direct_mode or "include").lower(),
            path_scope=(path_scope or "converted").lower(),
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            nba_config=get_settings_obj().nba.model_dump(),
        )

    @router.get("/api/paths/details")
    def get_path_details(
        path: str,
        config_id: Optional[str] = None,
        direct_mode: str = "include",
        path_scope: str = "converted",
        db=Depends(get_db_dependency),
    ):
        if not path:
            raise HTTPException(status_code=400, detail="path is required")

        journeys = get_journeys_fn(db)
        if not journeys:
            raise HTTPException(status_code=400, detail="No journeys loaded.")

        resolved_cfg, meta = load_config_and_meta_fn(db, config_id)
        journeys_for_analysis = apply_model_config_fn(journeys, resolved_cfg.config_json or {}) if resolved_cfg else journeys

        direct_mode_normalized = (direct_mode or "include").lower()
        if direct_mode_normalized not in ("include", "exclude"):
            direct_mode_normalized = "include"
        if direct_mode_normalized == "exclude":
            filtered_journeys = []
            for journey in journeys_for_analysis:
                kept = [tp for tp in journey.get("touchpoints", []) if tp.get("channel", "").lower() != "direct"]
                if not kept:
                    continue
                j2 = dict(journey)
                j2["touchpoints"] = kept
                filtered_journeys.append(j2)
            journeys_for_analysis = filtered_journeys

        include_non_converted = (path_scope or "converted").lower() in ("all", "all_journeys", "include_non_converted")
        journeys_universe = journeys_for_analysis if include_non_converted else [j for j in journeys_for_analysis if j.get("converted", True)]
        if not journeys_universe:
            raise HTTPException(status_code=400, detail="No journeys for this view.")

        target_steps = [s for s in path.split(" > ") if s]
        matching_journeys = []
        for journey in journeys_universe:
            steps = [step_string_fn(tp, "channel") for tp in journey.get("touchpoints", [])]
            if " > ".join(steps) == path:
                matching_journeys.append(journey)

        total_in_view = len(journeys_universe)
        count = len(matching_journeys)
        avg_len = 0.0
        times: List[float] = []
        for journey in matching_journeys:
            tps = journey.get("touchpoints", [])
            if tps:
                avg_len += len(tps)
            if journey.get("converted", True) and len(tps) >= 2:
                try:
                    first_ts = pd.Timestamp(tps[0].get("timestamp", ""))
                    last_ts = pd.Timestamp(tps[-1].get("timestamp", ""))
                    if pd.notna(first_ts) and pd.notna(last_ts):
                        delta = (last_ts - first_ts).total_seconds() / 86400.0
                        if delta >= 0:
                            times.append(delta)
                except Exception:
                    pass
        avg_len = avg_len / count if count else 0.0
        avg_time = sum(times) / len(times) if times else None

        step_breakdown = []
        if target_steps and count:
            for idx, step in enumerate(target_steps):
                pos = idx + 1
                prefix = target_steps[:pos]
                prefix_matches = 0
                stops_here = 0
                for journey in journeys_universe:
                    steps = [step_string_fn(tp, "channel") for tp in journey.get("touchpoints", [])]
                    if len(steps) < pos:
                        continue
                    if steps[:pos] == prefix:
                        prefix_matches += 1
                        if len(steps) == pos:
                            stops_here += 1
                dropoff_share = stops_here / prefix_matches if prefix_matches else 0.0
                step_breakdown.append({"step": step, "position": pos, "dropoff_share": round(dropoff_share, 4), "prefix_journeys": prefix_matches})

        variant_counts: Dict[str, int] = {}
        for journey in journeys_universe:
            steps = [step_string_fn(tp, "channel") for tp in journey.get("touchpoints", [])]
            candidate = " > ".join(steps)
            if candidate == path:
                continue
            variant_counts[candidate] = variant_counts.get(candidate, 0) + 1

        def _similarity_score(other: str) -> int:
            o_steps = [s for s in other.split(" > ") if s]
            score = 0
            for a, b in zip(target_steps, o_steps):
                if a == b:
                    score += 2
                else:
                    break
            if len(o_steps) == len(target_steps):
                score += 1
            elif abs(len(o_steps) - len(target_steps)) > 1:
                score -= 1
            return score

        variants = sorted(
            [{"path": pth, "count": c, "share": round(c / total_in_view, 4)} for pth, c in variant_counts.items()],
            key=lambda x: (_similarity_score(x["path"]), x["count"]),
            reverse=True,
        )[:5]

        direct_unknown_touches = 0
        total_touches = 0
        journeys_ending_direct = 0
        for journey in matching_journeys:
            tps = journey.get("touchpoints", [])
            for tp in tps:
                ch = tp.get("channel", "unknown")
                total_touches += 1
                if ch.lower() in ("direct", "unknown"):
                    direct_unknown_touches += 1
            if tps:
                last_ch = tps[-1].get("channel", "unknown")
                if last_ch and last_ch.lower() == "direct":
                    journeys_ending_direct += 1

        confidence = None
        if meta and meta.get("conversion_key"):
            snap = get_latest_quality_for_scope_fn(db=db, scope="channel", scope_id=None, conversion_key=meta["conversion_key"])
            if snap is not None:
                confidence = {
                    "score": float(snap.confidence_score),
                    "label": snap.confidence_label,
                    "components": snap.components_json or {},
                }

        return {
            "path": path,
            "summary": {
                "count": count,
                "share": round(count / total_in_view, 4) if total_in_view else 0.0,
                "avg_touchpoints": round(avg_len, 2),
                "avg_time_to_convert_days": round(avg_time, 2) if avg_time is not None else None,
            },
            "step_breakdown": step_breakdown,
            "variants": variants,
            "data_health": {
                "direct_unknown_touch_share": round(direct_unknown_touches / total_touches, 4) if total_touches else 0.0,
                "journeys_ending_direct_share": round(journeys_ending_direct / count, 4) if count else 0.0,
                "confidence": confidence,
            },
        }

    @router.get("/api/conversion-paths/details")
    def get_conversion_path_details_aggregated(
        path: str,
        definition_id: Optional[str] = Query(None, description="Optional journey definition id"),
        date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
        date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
        direct_mode: str = Query("include", description="include|exclude"),
        path_scope: str = Query("converted", description="converted|all"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        if not path:
            raise HTTPException(status_code=400, detail="path is required")
        d_from = None
        d_to = None
        try:
            if date_from:
                d_from = pd.to_datetime(date_from).date()
            if date_to:
                d_to = pd.to_datetime(date_to).date()
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
        if d_from and d_to and d_from > d_to:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")
        try:
            return build_conversion_path_details_from_daily_fn(
                db,
                path=path,
                definition_id=definition_id,
                date_from=d_from,
                date_to=d_to,
                direct_mode=(direct_mode or "include").lower(),
                path_scope=(path_scope or "converted").lower(),
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @router.get("/api/paths/archetypes")
    def get_path_archetypes(
        conversion_key: Optional[str] = None,
        config_id: Optional[str] = None,
        k_mode: str = "auto",
        k: Optional[int] = None,
        k_min: int = 3,
        k_max: int = 10,
        direct_mode: str = "include",
        compare_previous: bool = False,
        recompute: bool = False,
        db=Depends(get_db_dependency),
    ):
        journeys = get_journeys_fn(db)
        if not journeys:
            raise HTTPException(status_code=400, detail="No journeys loaded.")
        resolved_cfg, _meta = load_config_and_meta_fn(db, config_id)
        journeys_for_analysis = apply_model_config_fn(journeys, resolved_cfg.config_json or {}) if resolved_cfg else journeys
        direct_mode_normalized = (direct_mode or "include").lower()
        if direct_mode_normalized not in ("include", "exclude"):
            direct_mode_normalized = "include"
        if direct_mode_normalized == "exclude":
            filtered_journeys = []
            for journey in journeys_for_analysis:
                kept = [tp for tp in journey.get("touchpoints", []) if tp.get("channel", "").lower() != "direct"]
                if not kept:
                    continue
                j2 = dict(journey)
                j2["touchpoints"] = kept
                filtered_journeys.append(j2)
            journeys_for_analysis = filtered_journeys

        cache_key = (
            conversion_key or "",
            config_id or "",
            k_mode,
            int(k) if k is not None else None,
            int(k_min),
            int(k_max),
            direct_mode_normalized,
            bool(compare_previous),
            len(journeys_for_analysis),
        )
        if not recompute and cache_key in path_archetypes_cache_obj:
            return path_archetypes_cache_obj[cache_key]

        result = compute_path_archetypes_fn(
            journeys_for_analysis,
            conversion_key,
            k_mode=k_mode,
            k=k,
            k_min=k_min,
            k_max=k_max,
            enable_stability=True,
            enable_compare_previous=bool(compare_previous),
        )
        result.setdefault("diagnostics", {})
        result["diagnostics"]["view_filters"] = {"direct_mode": direct_mode_normalized}
        path_archetypes_cache_obj[cache_key] = result
        return result

    @router.get("/api/paths/anomalies")
    def get_path_anomalies(conversion_key: Optional[str] = None, config_id: Optional[str] = None, db=Depends(get_db_dependency)):
        journeys = get_journeys_fn(db)
        if not journeys:
            return {"anomalies": []}
        resolved_cfg, _meta = load_config_and_meta_fn(db, config_id)
        journeys_for_analysis = apply_model_config_fn(journeys, resolved_cfg.config_json or {}) if resolved_cfg else journeys
        return {"anomalies": compute_path_anomalies_fn(journeys_for_analysis, conversion_key)}

    def _next_best_action_impl(journeys: List[Dict[str, Any]], path_so_far: str = "", level: str = "channel"):
        if not journeys:
            raise HTTPException(status_code=400, detail="No journeys loaded.")
        prefix = path_so_far.strip().replace(",", " > ").replace("  ", " ").strip()
        use_level = "campaign" if level == "campaign" and has_any_campaign_fn(journeys) else "channel"
        nba_raw = compute_next_best_action_fn(journeys, level=use_level)
        filtered_map, _stats = filter_nba_recommendations_fn(nba_raw, get_settings_obj().nba)
        recs = filtered_map.get(prefix, [])
        filtered = recs[:10]
        why_samples = []
        if prefix:
            prefix_steps = [s for s in prefix.split(" > ") if s]
            path_counts: Dict[str, int] = {}
            direct_unknown_counts: Dict[str, int] = {}
            total_for_prefix = 0
            for journey in journeys:
                steps = [step_string_fn(tp, use_level) for tp in journey.get("touchpoints", [])]
                if len(steps) < len(prefix_steps) or steps[: len(prefix_steps)] != prefix_steps:
                    continue
                full_path = " > ".join(steps)
                path_counts[full_path] = path_counts.get(full_path, 0) + 1
                total_for_prefix += 1
                for step in steps:
                    if step.split(":", 1)[0].lower() in ("direct", "unknown"):
                        direct_unknown_counts[full_path] = direct_unknown_counts.get(full_path, 0) + 1
            if total_for_prefix:
                why_samples = sorted(
                    [
                        {
                            "path": pth,
                            "count": cnt,
                            "share": round(cnt / total_for_prefix, 4),
                            "direct_unknown_share": round(direct_unknown_counts.get(pth, 0) / (len(pth.split(" > ")) or 1), 4),
                        }
                        for pth, cnt in path_counts.items()
                    ],
                    key=lambda x: (x["count"], -x["direct_unknown_share"]),
                    reverse=True,
                )[:3]
        return {
            "path_so_far": prefix or "(start)",
            "level": use_level,
            "recommendations": filtered,
            "why_samples": why_samples,
            "nba_config": get_settings_obj().nba.model_dump(),
        }

    @router.get("/api/attribution/next-best-action")
    @router.get("/api/attribution/next_best_action")
    def get_next_best_action(path_so_far: str = "", level: str = "channel", db=Depends(get_db_dependency)):
        journeys = get_journeys_fn(db)
        if not journeys:
            raise HTTPException(status_code=400, detail="No journeys loaded.")
        return _next_best_action_impl(journeys=journeys, path_so_far=path_so_far, level=level)

    @router.get("/api/attribution/performance")
    def get_channel_performance(model: str = "linear", config_id: Optional[str] = None, db=Depends(get_db_dependency)):
        resolved_cfg, meta = load_config_and_meta_fn(db, config_id)
        journeys = get_journeys_fn(db)
        journeys_for_model = apply_model_config_fn(journeys, resolved_cfg.config_json or {}) if resolved_cfg else journeys
        journeys_for_model = _apply_attribution_filters(journeys_for_model)
        result = _results_store().get(model)
        if not result:
            if not journeys:
                raise HTTPException(status_code=400, detail="No journeys loaded.")
            settings = get_settings_obj()
            kwargs: Dict[str, Any] = {}
            if model == "time_decay":
                kwargs["half_life_days"] = settings.attribution.time_decay_half_life_days
            elif model == "position_based":
                kwargs["first_pct"] = settings.attribution.position_first_pct
                kwargs["last_pct"] = settings.attribution.position_last_pct
            result = run_attribution_fn(journeys_for_model, model=model, **kwargs)
            _results_store()[model] = result

        expense_by_channel: Dict[str, float] = {}
        for exp in expenses_obj.values():
            if getattr(exp, "status", "active") == "deleted":
                continue
            converted = exp.converted_amount if getattr(exp, "converted_amount", None) is not None else exp.amount
            expense_by_channel[exp.channel] = expense_by_channel.get(exp.channel, 0) + converted

        performance = compute_channel_performance_fn(result, expense_by_channel)
        for row in performance:
            snap = get_latest_quality_for_scope_fn(db, scope="channel", scope_id=row["channel"], conversion_key=meta["conversion_key"] if meta else None)
            if snap:
                row["confidence"] = {"score": snap.confidence_score, "label": snap.confidence_label, "components": snap.components_json}
        return {
            "model": model,
            "channels": performance,
            "total_spend": sum(expense_by_channel.values()),
            "total_attributed_value": result.get("total_value", 0),
            "total_conversions": result.get("total_conversions", 0),
            "config": meta,
        }

    @router.get("/api/attribution/campaign-performance")
    def get_campaign_performance(
        model: str = "linear",
        config_id: Optional[str] = None,
        conversion_key: Optional[str] = None,
        db=Depends(get_db_dependency),
    ):
        journeys = get_journeys_fn(db)
        if not journeys:
            raise HTTPException(status_code=400, detail="No journeys loaded.")
        if not has_any_campaign_fn(journeys):
            return {
                "model": model,
                "campaigns": [],
                "total_conversions": 0,
                "total_value": 0,
                "message": "No campaign data in touchpoints. Add a 'campaign' field to use campaign performance.",
            }
        resolved_cfg, meta = load_config_and_meta_fn(db, config_id)
        journeys_for_model = apply_model_config_fn(journeys, resolved_cfg.config_json or {}) if resolved_cfg else journeys
        journeys_for_model = _apply_attribution_filters(journeys_for_model)
        effective_conv = conversion_key or (meta.get("conversion_key") if meta else None)
        if effective_conv:
            journeys_for_model = [j for j in journeys_for_model if j.get("kpi_type") == effective_conv]

        settings = get_settings_obj()
        kwargs: Dict[str, Any] = {}
        if model == "time_decay":
            kwargs["half_life_days"] = settings.attribution.time_decay_half_life_days
        elif model == "position_based":
            kwargs["first_pct"] = settings.attribution.position_first_pct
            kwargs["last_pct"] = settings.attribution.position_last_pct
        result = run_attribution_campaign_fn(journeys_for_model, model=model, **kwargs)

        expense_by_channel: Dict[str, float] = {}
        for exp in expenses_obj.values():
            if getattr(exp, "status", "active") == "deleted":
                continue
            converted = exp.converted_amount if getattr(exp, "converted_amount", None) is not None else exp.amount
            expense_by_channel[exp.channel] = expense_by_channel.get(exp.channel, 0) + converted

        total_spend = sum(expense_by_channel.values())
        total_attributed_value = float(result.get("total_value", 0) or 0.0)
        mapped_spend = 0.0
        mapped_value = 0.0
        campaigns_list = []
        for ch in result.get("channels", []):
            step = ch["channel"]
            channel_name = step.split(":", 1)[0] if ":" in step else step
            campaign_name = step.split(":", 1)[1] if ":" in step else None
            spend = expense_by_channel.get(channel_name, 0)
            mapped_spend += spend
            attr_val = ch["attributed_value"]
            attr_conv = ch.get("attributed_conversions", 0)
            mapped_value += attr_val
            efficiency = derive_efficiency_fn(spend=float(spend or 0.0), conversions=float(attr_conv or 0.0), revenue=float(attr_val or 0.0))
            campaigns_list.append({
                "campaign": step,
                "channel": channel_name,
                "campaign_name": campaign_name,
                "attributed_value": ch["attributed_value"],
                "attributed_share": ch["attributed_share"],
                "attributed_conversions": ch.get("attributed_conversions", 0),
                "spend": round(spend, 2),
                "roi": round(efficiency["roi"], 4) if efficiency["roi"] is not None else None,
                "roas": round(efficiency["roas"], 2) if efficiency["roas"] is not None else None,
                "cpa": round(efficiency["cpa"], 2) if efficiency["cpa"] is not None else None,
            })

        nba_campaign_raw = compute_next_best_action_fn(journeys, level="campaign")
        nba_campaign, _nba_campaign_stats = filter_nba_recommendations_fn(nba_campaign_raw, get_settings_obj().nba)
        uplift = compute_campaign_uplift_fn(journeys_for_model)
        for campaign in campaigns_list:
            recs = nba_campaign.get(campaign["campaign"], [])
            campaign["suggested_next"] = recs[0] if recs else None
            u = uplift.get(campaign["campaign"])
            if u:
                campaign["treatment_rate"] = u["treatment_rate"]
                campaign["holdout_rate"] = u["holdout_rate"]
                campaign["uplift_abs"] = u["uplift_abs"]
                campaign["uplift_rel"] = u["uplift_rel"]
                campaign["treatment_n"] = u["treatment_n"]
                campaign["holdout_n"] = u["holdout_n"]
            snap = get_latest_quality_for_scope_fn(db, scope="campaign", scope_id=campaign["campaign"], conversion_key=meta["conversion_key"] if meta else None)
            if snap:
                campaign["confidence"] = {"score": snap.confidence_score, "label": snap.confidence_label, "components": snap.components_json}
                campaign["confidence_score"] = snap.confidence_score

        coverage_spend_pct = (mapped_spend / total_spend * 100.0) if total_spend > 0 else 0.0
        coverage_value_pct = (mapped_value / total_attributed_value * 100.0) if total_attributed_value > 0 else 0.0
        return {
            "model": model,
            "campaigns": campaigns_list,
            "total_conversions": result.get("total_conversions", 0),
            "total_value": total_attributed_value,
            "total_spend": total_spend,
            "config": meta,
            "mapping_coverage": {
                "spend_mapped_pct": coverage_spend_pct,
                "value_mapped_pct": coverage_value_pct,
                "spend_mapped": mapped_spend,
                "spend_total": total_spend,
                "value_mapped": mapped_value,
                "value_total": total_attributed_value,
            },
        }

    @router.get("/api/attribution/campaign-performance/trends")
    def get_campaign_performance_trends(db=Depends(get_db_dependency)):
        journeys = get_journeys_fn(db)
        if not journeys:
            raise HTTPException(status_code=400, detail="No journeys loaded.")
        return compute_campaign_trends_fn(journeys)

    return router
