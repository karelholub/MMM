import json
import os
import threading
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.attribution_engine import compute_next_best_action, has_any_campaign
from app.modules.settings.schemas import NBASettings, Settings
from app.services_canonical_facts import iter_canonical_conversion_rows
from app.services_import_runs import get_last_successful_run, get_runs as get_import_runs
from app.services_nba_defaults import filter_nba_recommendations
from app.services_activation_measurement import (
    build_activation_feedback_export,
    build_activation_feedback_recommendations,
    build_activation_object_registry,
    build_activation_measurement_evidence,
    build_activation_measurement_summary,
    list_activation_feedback_exports,
    record_activation_feedback_export,
)
from app.services_overview import (
    get_overview_drivers,
    get_overview_funnels,
    get_overview_summary,
    get_overview_trend_insights,
)
from app.services_performance_lag import build_scope_lag_summary
from app.services_performance_diagnostics import build_scope_diagnostics
from app.services_performance_helpers import _local_date_from_ts
from app.services_performance_trends import (
    build_campaign_aggregate_overlay,
    build_campaign_summary_response,
    build_campaign_trend_response,
    build_channel_aggregate_overlay,
    build_channel_summary_response,
    build_channel_trend_response,
)
from app.services_quality import load_config_and_meta
from app.utils.meiro_config import (
    expense_matches_target_site_scope,
    event_site_scope,
    filter_journeys_to_target_site_scope,
    get_event_archive_status,
    get_out_of_scope_campaign_labels,
    get_target_site_domains,
    journey_matches_target_site_scope,
    site_scope_is_strict,
)


_CONSISTENCY_CACHE: dict[tuple, tuple[float, tuple[dict[str, Any] | None, list[str]]]] = {}
_CONSISTENCY_CACHE_LOCK = threading.RLock()
_OVERVIEW_SUMMARY_CACHE: dict[tuple, tuple[float, dict[str, Any]]] = {}
_OVERVIEW_SUMMARY_CACHE_LOCK = threading.RLock()
_PERFORMANCE_SUMMARY_CACHE: dict[tuple, tuple[float, dict[str, Any]]] = {}
_PERFORMANCE_SUMMARY_CACHE_LOCK = threading.RLock()
_DATA_DIR = Path(__file__).resolve().parents[2] / "data"


def _selected_period_bounds(date_from: str, date_to: str) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(str(date_from)[:10]).replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.fromisoformat(str(date_to)[:10]).replace(hour=23, minute=59, second=59, microsecond=999999)
    if end < start:
        start, end = end.replace(hour=0, minute=0, second=0, microsecond=0), start.replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=999999,
        )
    return start, end


def _has_canonical_conversions(
    db: Any,
    *,
    date_from: str,
    date_to: str,
    conversion_key: Optional[str],
) -> bool:
    start, end = _selected_period_bounds(date_from, date_to)
    for _row in iter_canonical_conversion_rows(
        db,
        date_from=start,
        date_to=end,
        conversion_key=conversion_key,
    ):
        return True
    return False


def _resolve_effective_conversion_key(
    db: Any,
    *,
    requested_conversion_key: Optional[str],
    configured_conversion_key: Optional[str],
    date_from: str,
    date_to: str,
) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    requested_key = (requested_conversion_key or "").strip() or None
    if requested_key is not None:
        return requested_key, None
    configured_key = (configured_conversion_key or "").strip() or None
    if not configured_key:
        return None, None
    return None, {
        "requested_conversion_key": None,
        "configured_conversion_key": configured_key,
        "applied_conversion_key": None,
        "reason": "performance_defaults_to_all_conversions_until_user_selects_a_conversion_key",
    }


def _load_selected_config_meta(db: Any, model_id: Optional[str]) -> tuple[Any, Optional[dict[str, Any]]]:
    if not model_id:
        return None, None
    return load_config_and_meta(db, model_id)


def _normalize_performance_attribution_model(model: Optional[str]) -> str:
    value = (model or "linear").strip().lower()
    return value if value in {"last_touch", "first_touch", "linear", "time_decay", "position_based", "markov"} else "linear"


_SETTINGS_PATH = Path(__file__).resolve().parents[2] / "data" / "settings.json"


def _load_runtime_nba_settings() -> NBASettings:
    if _SETTINGS_PATH.exists():
        try:
            payload = json.loads(_SETTINGS_PATH.read_text())
            return Settings(**payload).nba
        except Exception:
            pass
    return NBASettings()


def _filter_journeys_for_campaign_suggestions(
    *,
    journeys: list[dict[str, Any]],
    date_from: str,
    date_to: str,
    timezone: str,
    channels: Optional[List[str]],
    conversion_key: Optional[str],
) -> list[dict[str, Any]]:
    start_d = datetime.fromisoformat(str(date_from)[:10]).date()
    end_d = datetime.fromisoformat(str(date_to)[:10]).date()
    allowed_channels = set(channels or [])
    filter_channels = bool(allowed_channels)
    selected: list[dict[str, Any]] = []

    for journey in journeys or []:
        if conversion_key:
            journey_key = str(journey.get("kpi_type") or journey.get("conversion_key") or "")
            if journey_key != conversion_key:
                continue
        touchpoints = journey.get("touchpoints") or []
        if not touchpoints:
            continue
        last_tp = touchpoints[-1] if isinstance(touchpoints[-1], dict) else {}
        channel = str(last_tp.get("channel") or "unknown")
        if filter_channels and channel not in allowed_channels:
            continue
        day = _local_date_from_ts(last_tp.get("timestamp") or last_tp.get("ts"), timezone)
        if day is None or day < start_d or day > end_d:
            continue
        selected.append(journey)
    return selected


def _scope_journeys_to_target_sites(journeys: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not site_scope_is_strict():
        return journeys, {
            "strict": False,
            "target_sites": get_target_site_domains(),
            "journeys_excluded": 0,
            "out_of_scope_hosts": [],
        }
    out_hosts: dict[str, int] = {}
    excluded_by_host = 0
    excluded_by_campaign = 0
    for journey in journeys or []:
        explicit_out_of_scope = False
        for touchpoint in journey.get("touchpoints") or []:
            if not isinstance(touchpoint, dict):
                continue
            scope = event_site_scope(touchpoint)
            if scope.get("status") != "out_of_scope":
                continue
            explicit_out_of_scope = True
            host = str(scope.get("host") or "unknown")
            out_hosts[host] = out_hosts.get(host, 0) + 1
        if explicit_out_of_scope:
            excluded_by_host += 1
        elif not journey_matches_target_site_scope(journey, allow_unknown=True):
            excluded_by_campaign += 1
    kept = filter_journeys_to_target_site_scope(journeys or [], allow_unknown=True)
    excluded = len(journeys or []) - len(kept)
    return kept, {
        "strict": True,
        "target_sites": get_target_site_domains(),
        "journeys_total": len(journeys or []),
        "journeys_kept": len(kept),
        "journeys_excluded": excluded,
        "journeys_excluded_by_host": excluded_by_host,
        "journeys_excluded_by_campaign": excluded_by_campaign,
        "out_of_scope_hosts": [
            {"host": host, "count": count}
            for host, count in sorted(out_hosts.items(), key=lambda item: item[1], reverse=True)[:10]
        ],
    }


def _normalized_campaign_label(value: Any) -> str:
    raw = str(value or "").strip()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    return " ".join(raw.lower().split())


def _campaign_item_is_out_of_scope(item: dict[str, Any], labels: set[str]) -> bool:
    candidates = {
        _normalized_campaign_label(item.get("campaign_id")),
        _normalized_campaign_label(item.get("campaign_name")),
        _normalized_campaign_label(item.get("campaign")),
    }
    return any(candidate and candidate in labels for candidate in candidates)


def _filter_out_of_scope_campaign_items(out: dict[str, Any]) -> dict[str, Any]:
    labels = get_out_of_scope_campaign_labels()
    out["scope_filter"] = {
        "out_of_scope_campaign_labels": len(labels),
        "campaign_rows_excluded": 0,
        "mode": "strict_target_site" if site_scope_is_strict() else "disabled",
    }
    if not labels:
        return out
    items = out.get("items")
    if not isinstance(items, list):
        return out
    filtered = [item for item in items if not (isinstance(item, dict) and _campaign_item_is_out_of_scope(item, labels))]
    excluded = len(items) - len(filtered)
    if excluded <= 0:
        return out
    out["scope_filter"]["campaign_rows_excluded"] = excluded
    out["items"] = filtered
    totals = out.get("totals") if isinstance(out.get("totals"), dict) else {}
    for bucket in ("current", "previous"):
        if bucket not in totals:
            continue
        totals[bucket] = {
            key: sum(float(((item.get(bucket) or {}) if isinstance(item, dict) else {}).get(key) or 0.0) for item in filtered)
            for key in ("spend", "visits", "conversions", "revenue")
        }
    notes = out.get("notes") if isinstance(out.get("notes"), list) else []
    notes.append(f"Excluded {excluded} campaign rows matched to out-of-scope Meiro archive traffic.")
    out["notes"] = notes
    out["totals"] = totals
    return out


def _build_meiro_measurement_scope_meta(
    *,
    site_scope_meta: dict[str, Any],
    campaign_scope_filter: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    event_archive_status = get_event_archive_status()
    source_scope = event_archive_status.get("source_scope") or {}
    archive_site_scope = event_archive_status.get("site_scope") or {}
    out_of_scope_campaign_labels = len(get_out_of_scope_campaign_labels()) if site_scope_is_strict() else 0
    campaign_rows_excluded = int((campaign_scope_filter or {}).get("campaign_rows_excluded") or 0)
    warnings: list[str] = []
    if str(source_scope.get("status") or "") == "legacy_unverified":
        warnings.append("Some Meiro raw-event archive batches are legacy/unverified.")
    if str(source_scope.get("status") or "") == "out_of_scope":
        warnings.append("Some Meiro raw-event archive batches are from a non-target Pipes instance.")
    if int(archive_site_scope.get("out_of_scope_site_events") or 0) > 0:
        warnings.append("Some raw events are outside the active target-site scope.")
    if campaign_rows_excluded > 0:
        warnings.append(f"{campaign_rows_excluded} campaign rows were excluded because they only matched out-of-scope Meiro evidence.")
    return {
        "strict": site_scope_is_strict(),
        "target_sites": get_target_site_domains(),
        "source_scope": source_scope,
        "event_archive_site_scope": archive_site_scope,
        "summary_site_scope": site_scope_meta,
        "out_of_scope_campaign_labels": out_of_scope_campaign_labels,
        "campaign_rows_excluded": campaign_rows_excluded,
        "warnings": warnings,
    }


def _scope_expenses_to_target_sites(expenses: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if not site_scope_is_strict():
        return expenses, {"strict": False, "expenses_excluded": 0}
    scoped: dict[str, Any] = {}
    excluded = 0
    excluded_amount = 0.0
    for key, expense in (expenses or {}).items():
        if expense_matches_target_site_scope(expense, allow_unknown=True):
            scoped[key] = expense
            continue
        excluded += 1
        try:
            excluded_amount += float(getattr(expense, "converted_amount", None) or getattr(expense, "amount", 0.0) or 0.0)
        except Exception:
            pass
    return scoped, {
        "strict": True,
        "expenses_total": len(expenses or {}),
        "expenses_kept": len(scoped),
        "expenses_excluded": excluded,
        "excluded_amount": round(excluded_amount, 2),
    }


def _build_campaign_suggestions_payload(
    *,
    journeys: list[dict[str, Any]],
    settings: NBASettings,
) -> Dict[str, Any]:
    if not journeys:
        return {"items": {}, "level": "campaign", "eligible_journeys": 0}
    if not has_any_campaign(journeys):
        return {
            "items": {},
            "level": "campaign",
            "eligible_journeys": len(journeys),
            "reason": "Campaign suggestions unavailable because journeys lack campaign data.",
        }

    nba_campaign_raw = compute_next_best_action(journeys, level="campaign")
    nba_campaign, _stats = filter_nba_recommendations(nba_campaign_raw, settings)
    return {
        "items": {prefix: recs[0] for prefix, recs in nba_campaign.items() if recs},
        "level": "campaign",
        "eligible_journeys": len(journeys),
    }


def _build_channel_suggestions_payload(
    *,
    journeys: list[dict[str, Any]],
    settings: NBASettings,
) -> Dict[str, Any]:
    if not journeys:
        return {"items": {}, "level": "channel", "eligible_journeys": 0}

    nba_channel_raw = compute_next_best_action(journeys, level="channel")
    nba_channel, _stats = filter_nba_recommendations(nba_channel_raw, settings)
    return {
        "items": {
            prefix: recs[0]
            for prefix, recs in nba_channel.items()
            if prefix and recs
        },
        "level": "channel",
        "eligible_journeys": len(journeys),
    }


def _add_summary_derivatives(items: list[dict], scope_type: str, diagnostics: dict[str, Any]) -> None:
    for item in items:
        current = item.get("current") or {}
        previous = item.get("previous") or {}
        visits = float(current.get("visits", 0.0) or 0.0)
        conversions = float(current.get("conversions", 0.0) or 0.0)
        revenue = float(current.get("revenue", 0.0) or 0.0)
        spend = float(current.get("spend", 0.0) or 0.0)
        derived = item.setdefault("derived", {})
        derived["cvr"] = round((conversions / visits), 4) if visits > 0 else None
        derived["cost_per_visit"] = round((spend / visits), 4) if visits > 0 else None
        derived["revenue_per_visit"] = round((revenue / visits), 4) if visits > 0 else None

        prev_visits = float(previous.get("visits", 0.0) or 0.0)
        prev_conversions = float(previous.get("conversions", 0.0) or 0.0)
        prev_revenue = float(previous.get("revenue", 0.0) or 0.0)
        prev_spend = float(previous.get("spend", 0.0) or 0.0)
        item["previous_derived"] = {
            "cvr": round((prev_conversions / prev_visits), 4) if prev_visits > 0 else None,
            "cost_per_visit": round((prev_spend / prev_visits), 4) if prev_visits > 0 else None,
            "revenue_per_visit": round((prev_revenue / prev_visits), 4) if prev_visits > 0 else None,
        } if previous else None

        if scope_type == "channel":
            diag_key = str(item.get("channel"))
        else:
            diag_key = str(item.get("campaign_id") or item.get("channel"))
        item["diagnostics"] = diagnostics.get(diag_key, {})


def _build_consistency_payload(db: Any, journeys: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
    cache_key = ("performance_consistency", len(journeys or []))
    now = time.monotonic()
    with _CONSISTENCY_CACHE_LOCK:
        cached = _CONSISTENCY_CACHE.get(cache_key)
        if cached and now - cached[0] < 60:
            return cached[1]
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
            get_import_runs_fn=get_import_runs,
            active_settings=active_settings,
            active_settings_preview=active_preview,
        )
        warnings = [
            *readiness.get("blockers", []),
            *readiness.get("warnings", []),
        ]
        payload = (readiness, warnings)
        with _CONSISTENCY_CACHE_LOCK:
            _CONSISTENCY_CACHE.clear()
            _CONSISTENCY_CACHE[cache_key] = (now, payload)
        return payload
    except Exception:
        return None, []


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    ensure_journeys_loaded_fn: Callable[[Any], list[dict]],
    get_overview_attention_queue_fn: Callable[[Any], list[dict[str, Any]]],
    build_query_context_fn: Callable[..., Any],
    build_meta_fn: Callable[..., dict[str, Any]],
    attach_scope_confidence_fn: Callable[..., None],
    build_mapping_coverage_fn: Callable[..., dict[str, Any]],
    summarize_mapped_current_fn: Callable[[list[dict]], dict[str, float]],
    expenses_obj: dict[str, Any],
) -> APIRouter:
    router = APIRouter(tags=["performance"])

    def _deciengine_events_config_path() -> Path:
        return _DATA_DIR / "deciengine_events_config.json"

    def _load_deciengine_source_url() -> str:
        path = _deciengine_events_config_path()
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(payload, dict) and str(payload.get("source_url") or "").strip():
                    return str(payload.get("source_url")).strip()
            except Exception:
                pass
        return "http://host.docker.internal:3001/v1/inapp/events"

    def _load_deciengine_api_key() -> Optional[str]:
        for key in ("DECIENGINE_API_KEY", "DECIENGINE_WRITE_KEY"):
            value = os.getenv(key)
            if value and value.strip():
                return value.strip()
        path = _deciengine_events_config_path()
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    for key in ("api_key", "write_key"):
                        value = payload.get(key)
                        if isinstance(value, str) and value.strip():
                            return value.strip()
            except Exception:
                pass
        return None

    def _activation_feedback_target_url(source_url: str) -> str:
        parsed = urllib.parse.urlparse(source_url)
        if parsed.scheme and parsed.netloc:
            base_path = parsed.path.split("/v1/", 1)[0]
            return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, f"{base_path}/v1/measurement/activation-feedback/import", "", "", ""))
        return source_url

    def _deliver_activation_feedback_to_deciengine(target_url: str, payload: dict[str, Any], api_key: str) -> dict[str, Any]:
        parsed = urllib.parse.urlparse(target_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return {
                "status": "failed",
                "reason": "deciEngine handoff URL must be an absolute http(s) URL.",
            }
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            target_url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-API-Key": api_key,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                response_body = response.read(65536).decode("utf-8", errors="replace")
                receiver_payload = json.loads(response_body) if response_body else {}
                return {
                    "status": "delivered",
                    "receiver_status": response.status,
                    "receiver_run": receiver_payload.get("run") if isinstance(receiver_payload, dict) else None,
                    "receiver_summary": receiver_payload.get("summary") if isinstance(receiver_payload, dict) else None,
                }
        except urllib.error.HTTPError as exc:
            response_body = exc.read(4096).decode("utf-8", errors="replace")
            return {
                "status": "failed",
                "receiver_status": exc.code,
                "reason": response_body or str(exc),
            }
        except Exception as exc:
            return {
                "status": "failed",
                "reason": str(exc),
            }

    @router.get("/api/overview/summary")
    def overview_summary(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="Timezone for display"),
        currency: Optional[str] = Query(None, description="Filter/display currency"),
        workspace: Optional[str] = Query(None, description="Workspace filter"),
        account: Optional[str] = Query(None, description="Account filter"),
        model_id: Optional[str] = Query(None, description="Optional model config id (for metadata only)"),
        channel_group: Optional[str] = Query(None, description="Optional saved-segment-compatible channel group filter"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        cache_key = (
            "overview_summary",
            date_from,
            date_to,
            timezone,
            currency or "",
            workspace or "",
            account or "",
            model_id or "",
            channel_group or "",
        )
        now = time.monotonic()
        with _OVERVIEW_SUMMARY_CACHE_LOCK:
            cached = _OVERVIEW_SUMMARY_CACHE.get(cache_key)
            if cached and now - cached[0] < 30:
                return cached[1]
        payload = get_overview_summary(
            db=db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            channel_group=channel_group,
            expenses=_scope_expenses_to_target_sites(expenses_obj)[0],
            import_runs_get_last_successful=get_last_successful_run,
        )
        payload["attention_queue"] = get_overview_attention_queue_fn(db)
        with _OVERVIEW_SUMMARY_CACHE_LOCK:
            _OVERVIEW_SUMMARY_CACHE.clear()
            _OVERVIEW_SUMMARY_CACHE[cache_key] = (now, payload)
        return payload

    @router.get("/api/measurement/activation-summary")
    def activation_measurement_summary(
        object_type: str = Query(
            ...,
            description=(
                "campaign | decision | decision_stack | asset | offer | content | bundle | "
                "experiment | variant | placement | template"
            ),
        ),
        object_id: str = Query(..., description="Activation object identifier to match"),
        date_from: Optional[str] = Query(None, description="Optional start date (YYYY-MM-DD)"),
        date_to: Optional[str] = Query(None, description="Optional end date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        activation_campaign_id: Optional[str] = Query(None, description="Optional deciEngine activation campaign alias"),
        native_meiro_campaign_id: Optional[str] = Query(None, description="Optional native Meiro campaign alias"),
        creative_asset_id: Optional[str] = Query(None, description="Optional creative/content asset alias"),
        native_meiro_asset_id: Optional[str] = Query(None, description="Optional native Meiro asset alias"),
        offer_catalog_id: Optional[str] = Query(None, description="Optional offer catalog alias"),
        native_meiro_catalog_id: Optional[str] = Query(None, description="Optional native Meiro catalog alias"),
        segment_id: Optional[str] = Query(None, description="Optional Meiro/MMM segment or audience id scope"),
        segment_alias: Optional[List[str]] = Query(None, description="Optional segment aliases to match"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, _site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        match_aliases = [
            activation_campaign_id,
            native_meiro_campaign_id,
            creative_asset_id,
            native_meiro_asset_id,
            offer_catalog_id,
            native_meiro_catalog_id,
        ]
        try:
            return build_activation_measurement_summary(
                journeys=journeys,
                object_type=object_type,
                object_id=object_id,
                match_aliases=match_aliases,
                date_from=date_from,
                date_to=date_to,
                conversion_key=conversion_key,
                segment_id=segment_id,
                segment_aliases=segment_alias,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/measurement/activation-objects")
    def activation_measurement_objects(
        object_type: Optional[str] = Query(
            None,
            description=(
                "Optional object type filter: campaign | decision | decision_stack | asset | offer | content | "
                "bundle | experiment | variant | placement | template"
            ),
        ),
        q: Optional[str] = Query(None, description="Optional substring search over object ids, labels, and aliases"),
        limit: int = Query(50, ge=1, le=200, description="Maximum objects to return"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, _site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        try:
            return build_activation_object_registry(journeys=journeys, object_type=object_type, q=q, limit=limit)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/measurement/activation-feedback")
    def activation_measurement_feedback(
        limit: int = Query(10, ge=1, le=50, description="Maximum feedback recommendations to return"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, _site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        return build_activation_feedback_recommendations(journeys=journeys, limit=limit)

    @router.get("/api/measurement/activation-feedback/export")
    def activation_measurement_feedback_export(
        limit: int = Query(50, ge=1, le=200, description="Maximum exported activation feedback signals"),
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        return build_activation_feedback_export(
            journeys=journeys,
            limit=limit,
            generated_by=getattr(ctx, "user_id", None) or "mmm",
        )

    @router.post("/api/measurement/activation-feedback/exports")
    def create_activation_measurement_feedback_export(
        limit: int = Query(50, ge=1, le=200, description="Maximum exported activation feedback signals"),
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, _site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        return record_activation_feedback_export(
            journeys=journeys,
            limit=limit,
            generated_by=getattr(ctx, "user_id", None) or "mmm",
        )

    @router.post("/api/measurement/activation-feedback/deciengine-handoff")
    def create_activation_measurement_feedback_deciengine_handoff(
        limit: int = Query(50, ge=1, le=200, description="Maximum exported activation feedback signals"),
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, _site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        run = record_activation_feedback_export(
            journeys=journeys,
            limit=limit,
            generated_by=getattr(ctx, "user_id", None) or "mmm",
        )
        source_url = _load_deciengine_source_url()
        target_url = _activation_feedback_target_url(source_url)
        api_key = _load_deciengine_api_key()
        delivery = (
            _deliver_activation_feedback_to_deciengine(target_url, run.get("payload") or {}, api_key)
            if api_key
            else {
                "status": "not_configured",
                "reason": "Set DECIENGINE_API_KEY or DECIENGINE_WRITE_KEY in the MMM API container to enable automatic delivery.",
            }
        )
        status = "delivered" if delivery.get("status") == "delivered" else "ready"
        return {
            "status": status,
            "message": (
                "Activation feedback delivered to deciEngine."
                if status == "delivered"
                else "Activation feedback export is ready for deciEngine import."
            ),
            "target": {
                "system": "deciEngine",
                "source_url": source_url,
                "handoff_url": target_url,
                "receiver_available": True,
                "authenticated_delivery_configured": bool(api_key),
                "receiver_note": (
                    "Automatic POST delivery is enabled."
                    if api_key
                    else "Set DECIENGINE_API_KEY or DECIENGINE_WRITE_KEY in the MMM API container to enable automatic POST delivery."
                ),
            },
            "delivery": delivery,
            "run": run,
        }

    @router.get("/api/measurement/activation-feedback/exports")
    def list_activation_measurement_feedback_exports(
        limit: int = Query(20, ge=1, le=50, description="Maximum export runs to return"),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return list_activation_feedback_exports(limit=limit)

    @router.get("/api/measurement/activation-evidence")
    def activation_measurement_evidence(
        object_type: str = Query(
            ...,
            description=(
                "campaign | decision | decision_stack | asset | offer | content | bundle | "
                "experiment | variant | placement | template"
            ),
        ),
        object_id: str = Query(..., description="Activation object identifier to match"),
        date_from: Optional[str] = Query(None, description="Optional start date (YYYY-MM-DD)"),
        date_to: Optional[str] = Query(None, description="Optional end date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        activation_campaign_id: Optional[str] = Query(None, description="Optional deciEngine activation campaign alias"),
        native_meiro_campaign_id: Optional[str] = Query(None, description="Optional native Meiro campaign alias"),
        creative_asset_id: Optional[str] = Query(None, description="Optional creative/content asset alias"),
        native_meiro_asset_id: Optional[str] = Query(None, description="Optional native Meiro asset alias"),
        offer_catalog_id: Optional[str] = Query(None, description="Optional offer catalog alias"),
        native_meiro_catalog_id: Optional[str] = Query(None, description="Optional native Meiro catalog alias"),
        segment_id: Optional[str] = Query(None, description="Optional Meiro/MMM segment or audience id scope"),
        segment_alias: Optional[List[str]] = Query(None, description="Optional segment aliases to match"),
        limit: int = Query(20, ge=1, le=100, description="Maximum evidence rows to return"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, _site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        match_aliases = [
            activation_campaign_id,
            native_meiro_campaign_id,
            creative_asset_id,
            native_meiro_asset_id,
            offer_catalog_id,
            native_meiro_catalog_id,
        ]
        try:
            return build_activation_measurement_evidence(
                journeys=journeys,
                object_type=object_type,
                object_id=object_id,
                match_aliases=match_aliases,
                date_from=date_from,
                date_to=date_to,
                conversion_key=conversion_key,
                segment_id=segment_id,
                segment_aliases=segment_alias,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/overview/drivers")
    def overview_drivers(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        top_campaigns_n: int = Query(10, ge=1, le=50, description="Top N campaigns"),
        conversion_key: Optional[str] = Query(None, description="Filter by conversion key"),
        channel_group: Optional[str] = Query(None, description="Optional saved-segment-compatible channel group filter"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return get_overview_drivers(
            db=db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            expenses=_scope_expenses_to_target_sites(expenses_obj)[0],
            top_campaigns_n=top_campaigns_n,
            conversion_key=conversion_key,
            channel_group=channel_group,
        )

    @router.get("/api/overview/funnels")
    def overview_funnels(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        limit: int = Query(5, ge=1, le=10, description="Rows per tab"),
        channel_group: Optional[str] = Query(None, description="Optional saved-segment-compatible channel group filter"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return get_overview_funnels(
            db=db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            conversion_key=conversion_key,
            limit=limit,
            channel_group=channel_group,
        )

    @router.get("/api/overview/trends")
    def overview_trends(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        channel_group: Optional[str] = Query(None, description="Optional saved-segment-compatible channel group filter"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return get_overview_trend_insights(
            db=db,
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            conversion_key=conversion_key,
            channel_group=channel_group,
        )

    @router.get("/api/performance/channel/trend")
    def performance_channel_trend(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        model: str = Query("linear", description="Attribution model for revenue/conversion credit"),
        kpi_key: str = Query("revenue", description="spend|visits|conversions|revenue|cpa|roas"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        grain: str = Query("auto", description="auto|daily|weekly"),
        compare: bool = Query(True, description="Include previous-period series"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        try:
            query_ctx = build_query_context_fn(
                date_from=date_from,
                date_to=date_to,
                timezone=timezone,
                currency=currency,
                workspace=workspace,
                account=account,
                model_id=model_id,
                kpi_key=kpi_key,
                grain=grain,
                compare=compare,
                channels=channels,
                conversion_key=conversion_key,
            )
            _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
            effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
                db,
                requested_conversion_key=query_ctx.conversion_key,
                configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
            )
            attribution_model = _normalize_performance_attribution_model(model)
            aggregate_overlay = None
            if attribution_model == "last_touch":
                aggregate_overlay = build_channel_aggregate_overlay(
                    db,
                    date_from=query_ctx.date_from,
                    date_to=query_ctx.date_to,
                    timezone=query_ctx.timezone,
                    compare=query_ctx.compare,
                    channels=query_ctx.channels,
                    conversion_key=effective_conversion_key,
                    grain=query_ctx.grain,
                )
            out = build_channel_trend_response(
                journeys=journeys,
                expenses=_scope_expenses_to_target_sites(expenses_obj)[0],
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
                timezone=query_ctx.timezone,
                kpi_key=query_ctx.kpi_key,
                grain=query_ctx.grain,
                compare=query_ctx.compare,
                channels=query_ctx.channels,
                conversion_key=effective_conversion_key,
                attribution_model=attribution_model,
                aggregate_overlay=aggregate_overlay,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key, include_kpi=True)
        out["meta"]["attribution_model"] = _normalize_performance_attribution_model(model)
        out["meta"]["site_scope"] = site_scope_meta
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        return out

    @router.get("/api/performance/channel/summary")
    def performance_channel_summary(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        model: str = Query("linear", description="Attribution model for revenue/conversion credit"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        compare: bool = Query(True, description="Include previous-period summary"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        cache_key = (
            "channel_summary",
            date_from,
            date_to,
            timezone,
            currency or "",
            workspace or "",
            account or "",
            model_id or "",
            _normalize_performance_attribution_model(model),
            conversion_key or "",
            bool(compare),
            tuple(channels or []),
        )
        now = time.monotonic()
        with _PERFORMANCE_SUMMARY_CACHE_LOCK:
            cached = _PERFORMANCE_SUMMARY_CACHE.get(cache_key)
            if cached and now - cached[0] < 30:
                return cached[1]
        journeys, site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        query_ctx = build_query_context_fn(
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            kpi_key="revenue",
            grain="daily",
            compare=compare,
            channels=channels,
            conversion_key=conversion_key,
        )
        _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
        effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
            db,
            requested_conversion_key=query_ctx.conversion_key,
            configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
        )
        attribution_model = _normalize_performance_attribution_model(model)
        aggregate_overlay = None
        if attribution_model == "last_touch":
            aggregate_overlay = build_channel_aggregate_overlay(
                db,
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
                timezone=query_ctx.timezone,
                compare=query_ctx.compare,
                channels=query_ctx.channels,
                conversion_key=effective_conversion_key,
                grain="daily",
            )
        out = build_channel_summary_response(
            journeys=journeys,
            expenses=_scope_expenses_to_target_sites(expenses_obj)[0],
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            compare=query_ctx.compare,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
            attribution_model=attribution_model,
            aggregate_overlay=aggregate_overlay,
        )
        diagnostics = build_scope_diagnostics(
            db=db,
            scope_type="channel",
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            conversion_key=effective_conversion_key,
            channels=query_ctx.channels,
        )
        _add_summary_derivatives(out.get("items", []), "channel", diagnostics)
        attach_scope_confidence_fn(
            db=db,
            items=out.get("items", []),
            scope_type="channel",
            id_field="channel",
            conversion_key=effective_conversion_key,
        )
        mapped = summarize_mapped_current_fn(out.get("items", []))
        out["config"] = config_meta
        out["mapping_coverage"] = build_mapping_coverage_fn(
            mapped_spend=mapped["mapped_spend"],
            mapped_value=mapped["mapped_value"],
            expenses=_scope_expenses_to_target_sites(expenses_obj)[0],
            journeys=journeys,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone_name=query_ctx.timezone,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        readiness, consistency_warnings = _build_consistency_payload(db, journeys)
        out["readiness"] = readiness
        out["consistency_warnings"] = consistency_warnings
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        out["meta"]["attribution_model"] = attribution_model
        out["meta"]["site_scope"] = site_scope_meta
        out["meta"]["meiro_measurement_scope"] = _build_meiro_measurement_scope_meta(
            site_scope_meta=site_scope_meta,
        )
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        with _PERFORMANCE_SUMMARY_CACHE_LOCK:
            _PERFORMANCE_SUMMARY_CACHE[cache_key] = (now, out)
        return out

    @router.get("/api/performance/channel/lag")
    def performance_channel_lag(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return build_scope_lag_summary(
            db,
            scope_type="channel",
            date_from=date_from,
            date_to=date_to,
            conversion_key=conversion_key,
            channels=channels,
        )

    @router.get("/api/performance/campaign/trend")
    def performance_campaign_trend(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        model: str = Query("linear", description="Attribution model for revenue/conversion credit"),
        kpi_key: str = Query("revenue", description="spend|visits|conversions|revenue|cpa|roas"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        grain: str = Query("auto", description="auto|daily|weekly"),
        compare: bool = Query(True, description="Include previous-period series"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        try:
            query_ctx = build_query_context_fn(
                date_from=date_from,
                date_to=date_to,
                timezone=timezone,
                currency=currency,
                workspace=workspace,
                account=account,
                model_id=model_id,
                kpi_key=kpi_key,
                grain=grain,
                compare=compare,
                channels=channels,
                conversion_key=conversion_key,
            )
            _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
            effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
                db,
                requested_conversion_key=query_ctx.conversion_key,
                configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
            )
            attribution_model = _normalize_performance_attribution_model(model)
            aggregate_overlay = None
            if attribution_model == "last_touch":
                aggregate_overlay = build_campaign_aggregate_overlay(
                    db,
                    date_from=query_ctx.date_from,
                    date_to=query_ctx.date_to,
                    timezone=query_ctx.timezone,
                    compare=query_ctx.compare,
                    channels=query_ctx.channels,
                    conversion_key=effective_conversion_key,
                )
            out = build_campaign_trend_response(
                journeys=journeys,
                expenses=_scope_expenses_to_target_sites(expenses_obj)[0],
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
                timezone=query_ctx.timezone,
                kpi_key=query_ctx.kpi_key,
                grain=query_ctx.grain,
                compare=query_ctx.compare,
                channels=query_ctx.channels,
                conversion_key=effective_conversion_key,
                attribution_model=attribution_model,
                aggregate_overlay=aggregate_overlay,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key, include_kpi=True)
        out["meta"]["attribution_model"] = _normalize_performance_attribution_model(model)
        out["meta"]["site_scope"] = site_scope_meta
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        return out

    @router.get("/api/performance/campaign/lag")
    def performance_campaign_lag(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        return build_scope_lag_summary(
            db,
            scope_type="campaign",
            date_from=date_from,
            date_to=date_to,
            conversion_key=conversion_key,
            channels=channels,
        )

    @router.get("/api/performance/campaign/summary")
    def performance_campaign_summary(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        model: str = Query("linear", description="Attribution model for revenue/conversion credit"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        compare: bool = Query(True, description="Include previous-period summary"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        cache_key = (
            "campaign_summary",
            date_from,
            date_to,
            timezone,
            currency or "",
            workspace or "",
            account or "",
            model_id or "",
            _normalize_performance_attribution_model(model),
            conversion_key or "",
            bool(compare),
            tuple(channels or []),
        )
        now = time.monotonic()
        with _PERFORMANCE_SUMMARY_CACHE_LOCK:
            cached = _PERFORMANCE_SUMMARY_CACHE.get(cache_key)
            if cached and now - cached[0] < 30:
                return cached[1]
        journeys, site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        query_ctx = build_query_context_fn(
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            kpi_key="revenue",
            grain="daily",
            compare=compare,
            channels=channels,
            conversion_key=conversion_key,
        )
        _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
        effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
            db,
            requested_conversion_key=query_ctx.conversion_key,
            configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
        )
        attribution_model = _normalize_performance_attribution_model(model)
        aggregate_overlay = None
        if attribution_model == "last_touch":
            aggregate_overlay = build_campaign_aggregate_overlay(
                db,
                date_from=query_ctx.date_from,
                date_to=query_ctx.date_to,
                timezone=query_ctx.timezone,
                compare=query_ctx.compare,
                channels=query_ctx.channels,
                conversion_key=effective_conversion_key,
            )
        out = build_campaign_summary_response(
            journeys=journeys,
            expenses=_scope_expenses_to_target_sites(expenses_obj)[0],
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            compare=query_ctx.compare,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
            attribution_model=attribution_model,
            aggregate_overlay=aggregate_overlay,
        )
        out = _filter_out_of_scope_campaign_items(out)
        diagnostics = build_scope_diagnostics(
            db=db,
            scope_type="campaign",
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            conversion_key=effective_conversion_key,
            channels=query_ctx.channels,
        )
        _add_summary_derivatives(out.get("items", []), "campaign", diagnostics)
        attach_scope_confidence_fn(
            db=db,
            items=out.get("items", []),
            scope_type="campaign",
            id_field="campaign_id",
            conversion_key=effective_conversion_key,
        )
        mapped = summarize_mapped_current_fn(out.get("items", []))
        out["config"] = config_meta
        out["mapping_coverage"] = build_mapping_coverage_fn(
            mapped_spend=mapped["mapped_spend"],
            mapped_value=mapped["mapped_value"],
            expenses=_scope_expenses_to_target_sites(expenses_obj)[0],
            journeys=journeys,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone_name=query_ctx.timezone,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        readiness, consistency_warnings = _build_consistency_payload(db, journeys)
        out["readiness"] = readiness
        out["consistency_warnings"] = consistency_warnings
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        out["meta"]["attribution_model"] = attribution_model
        out["meta"]["site_scope"] = site_scope_meta
        out["meta"]["meiro_measurement_scope"] = _build_meiro_measurement_scope_meta(
            site_scope_meta=site_scope_meta,
            campaign_scope_filter=out.get("scope_filter") if isinstance(out.get("scope_filter"), dict) else None,
        )
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        with _PERFORMANCE_SUMMARY_CACHE_LOCK:
            _PERFORMANCE_SUMMARY_CACHE[cache_key] = (now, out)
        return out

    @router.get("/api/performance/campaign/suggestions")
    def performance_campaign_suggestions(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys, site_scope_meta = _scope_journeys_to_target_sites(ensure_journeys_loaded_fn(db))
        query_ctx = build_query_context_fn(
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            kpi_key="revenue",
            grain="daily",
            compare=False,
            channels=channels,
            conversion_key=conversion_key,
        )
        _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
        effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
            db,
            requested_conversion_key=query_ctx.conversion_key,
            configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
        )
        suggestion_journeys = _filter_journeys_for_campaign_suggestions(
            journeys=journeys,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        out = _build_campaign_suggestions_payload(
            journeys=suggestion_journeys,
            settings=_load_runtime_nba_settings(),
        )
        out["config"] = config_meta
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        out["meta"]["site_scope"] = site_scope_meta
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        return out

    @router.get("/api/performance/channel/suggestions")
    def performance_channel_suggestions(
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        timezone: str = Query("UTC", description="IANA timezone for bucketing"),
        currency: Optional[str] = Query(None, description="Display currency (metadata only)"),
        workspace: Optional[str] = Query(None, description="Workspace filter (reserved)"),
        account: Optional[str] = Query(None, description="Account filter (reserved)"),
        model_id: Optional[str] = Query(None, description="Optional model config id"),
        conversion_key: Optional[str] = Query(None, description="Optional conversion key filter"),
        channels: Optional[List[str]] = Query(None, description="Optional channel filter list"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("attribution.view")),
    ):
        journeys = ensure_journeys_loaded_fn(db)
        query_ctx = build_query_context_fn(
            date_from=date_from,
            date_to=date_to,
            timezone=timezone,
            currency=currency,
            workspace=workspace,
            account=account,
            model_id=model_id,
            kpi_key="revenue",
            grain="daily",
            compare=False,
            channels=channels,
            conversion_key=conversion_key,
        )
        _resolved_cfg, config_meta = _load_selected_config_meta(db, query_ctx.model_id)
        effective_conversion_key, conversion_key_resolution = _resolve_effective_conversion_key(
            db,
            requested_conversion_key=query_ctx.conversion_key,
            configured_conversion_key=(config_meta.get("conversion_key") if config_meta else None),
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
        )
        suggestion_journeys = _filter_journeys_for_campaign_suggestions(
            journeys=journeys,
            date_from=query_ctx.date_from,
            date_to=query_ctx.date_to,
            timezone=query_ctx.timezone,
            channels=query_ctx.channels,
            conversion_key=effective_conversion_key,
        )
        out = _build_channel_suggestions_payload(
            journeys=suggestion_journeys,
            settings=_load_runtime_nba_settings(),
        )
        out["config"] = config_meta
        out["meta"] = build_meta_fn(ctx=query_ctx, conversion_key=effective_conversion_key)
        if conversion_key_resolution:
            out["meta"]["conversion_key_resolution"] = conversion_key_resolution
        return out

    return router
