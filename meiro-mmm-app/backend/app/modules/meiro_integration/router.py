import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request

from app.connectors import meiro_cdp
from app.modules.meiro_integration.schemas import (
    MeiroCDPConnectRequest,
    MeiroCDPExportRequest,
    MeiroCDPTestRequest,
    MeiroMappingApprovalRequest,
    MeiroWebhookReprocessRequest,
)
from app.utils.meiro_config import (
    append_webhook_archive_entry,
    append_webhook_event,
    get_last_test_at,
    get_mapping,
    get_mapping_state,
    get_pull_config,
    get_webhook_archive_entries,
    get_webhook_archive_status,
    get_webhook_events,
    get_webhook_last_received_at,
    get_webhook_received_count,
    get_webhook_secret,
    rebuild_profiles_from_webhook_archive,
    rotate_webhook_secret,
    save_mapping,
    save_pull_config,
    set_webhook_received,
    update_mapping_approval,
)
from app.services_meiro_readiness import build_meiro_readiness
from app.utils.taxonomy import load_taxonomy


MEIRO_MAPPING_PRESETS = {
    "web_ads_ga": {
        "name": "Web + Ads (GA-like)",
        "touchpoint_attr": "touchpoints",
        "value_attr": "conversion_value",
        "id_attr": "customer_id",
        "channel_field": "channel",
        "timestamp_field": "timestamp",
        "channel_mapping": {
            "google": "google_ads",
            "facebook": "meta_ads",
            "meta": "meta_ads",
            "linkedin": "linkedin_ads",
            "cpc": "paid_search",
            "ppc": "paid_search",
            "organic": "organic_search",
            "email": "email",
            "direct": "direct",
        },
    },
    "crm_lifecycle": {
        "name": "CRM + Lifecycle",
        "touchpoint_attr": "touchpoints",
        "value_attr": "conversion_value",
        "id_attr": "customer_id",
        "channel_field": "source",
        "timestamp_field": "event_date",
        "channel_mapping": {
            "salesforce": "crm",
            "hubspot": "crm",
            "marketo": "marketing_automation",
            "pardot": "marketing_automation",
            "newsletter": "email",
            "onboarding": "lifecycle",
            "retention": "lifecycle",
            "winback": "lifecycle",
        },
    },
}


def _safe_json_excerpt(value: Any, max_chars: int = 20000) -> tuple[str, bool, int]:
    try:
        raw = json.dumps(value, ensure_ascii=False, indent=2)
    except Exception:
        raw = str(value)
    raw_bytes = len(raw.encode("utf-8", errors="ignore"))
    if len(raw) <= max_chars:
        return raw, False, raw_bytes
    suffix = "\n... [truncated]"
    return f"{raw[:max_chars]}{suffix}", True, raw_bytes


def _extract_payload_hints(payload_profiles: list[Any]) -> tuple[list[str], list[str]]:
    conversion_names: set[str] = set()
    channels: set[str] = set()
    for profile in payload_profiles[:100]:
        if not isinstance(profile, dict):
            continue
        for conversion in (profile.get("conversions") or [])[:100]:
            if not isinstance(conversion, dict):
                continue
            name = conversion.get("name")
            if isinstance(name, str) and name.strip():
                conversion_names.add(name.strip())
        for touchpoint in (profile.get("touchpoints") or [])[:100]:
            if not isinstance(touchpoint, dict):
                continue
            channel = touchpoint.get("channel")
            if isinstance(channel, str) and channel.strip():
                channels.add(channel.strip())
    return sorted(conversion_names)[:20], sorted(channels)[:20]


def _analyze_payload(payload_profiles: list[Any]) -> Dict[str, Any]:
    conversion_event_counts: Dict[str, int] = {}
    channel_counts: Dict[str, int] = {}
    source_counts: Dict[str, int] = {}
    medium_counts: Dict[str, int] = {}
    campaign_counts: Dict[str, int] = {}
    source_medium_pair_counts: Dict[str, int] = {}
    value_field_counts: Dict[str, int] = {}
    currency_field_counts: Dict[str, int] = {}
    touchpoint_attr_counts: Dict[str, int] = {}
    channel_field_path_counts: Dict[str, int] = {}
    source_field_path_counts: Dict[str, int] = {}
    medium_field_path_counts: Dict[str, int] = {}
    campaign_field_path_counts: Dict[str, int] = {}
    dedup_key_counts: Dict[str, int] = {"conversion_id": 0, "order_id": 0, "event_id": 0}
    conversion_count = 0
    touchpoint_count = 0

    def _inc(target: Dict[str, int], key: Optional[str]) -> None:
        if not isinstance(key, str):
            return
        normalized = key.strip()
        if not normalized:
            return
        target[normalized] = target.get(normalized, 0) + 1

    def _normalized_text(value: Any) -> Optional[str]:
        if isinstance(value, dict):
            value = value.get("name") or value.get("platform") or value.get("campaign_name")
        if not isinstance(value, str):
            return None
        normalized = value.strip().lower()
        return normalized or None

    for profile in payload_profiles[:200]:
        if not isinstance(profile, dict):
            continue
        conversions = profile.get("conversions") or []
        if isinstance(conversions, list):
            for conversion in conversions[:300]:
                if not isinstance(conversion, dict):
                    continue
                conversion_count += 1
                _inc(conversion_event_counts, str(conversion.get("name") or "").strip().lower())
                for dedup_key in ("conversion_id", "order_id", "event_id"):
                    if conversion.get(dedup_key):
                        dedup_key_counts[dedup_key] = dedup_key_counts.get(dedup_key, 0) + 1
                for key, value in conversion.items():
                    if key in {"id", "name", "ts", "timestamp"}:
                        continue
                    if isinstance(value, (int, float)):
                        value_field_counts[key] = value_field_counts.get(key, 0) + 1
                    elif isinstance(value, str):
                        raw = value.strip()
                        if len(raw) == 3 and raw.upper() == raw and raw.isalpha():
                            currency_field_counts[key] = currency_field_counts.get(key, 0) + 1

        touchpoints = profile.get("touchpoints") or []
        if isinstance(touchpoints, list):
            touchpoint_attr_counts["touchpoints"] = touchpoint_attr_counts.get("touchpoints", 0) + 1
            for tp in touchpoints[:500]:
                if not isinstance(tp, dict):
                    continue
                touchpoint_count += 1
                raw_source = _normalized_text(tp.get("source"))
                raw_medium = _normalized_text(tp.get("medium"))
                raw_campaign = _normalized_text(tp.get("campaign"))
                if tp.get("channel"):
                    channel_field_path_counts["channel"] = channel_field_path_counts.get("channel", 0) + 1
                if tp.get("source"):
                    source_field_path_counts["source"] = source_field_path_counts.get("source", 0) + 1
                if tp.get("medium"):
                    medium_field_path_counts["medium"] = medium_field_path_counts.get("medium", 0) + 1
                if tp.get("campaign"):
                    campaign_field_path_counts["campaign"] = campaign_field_path_counts.get("campaign", 0) + 1
                _inc(channel_counts, tp.get("channel"))
                utm = tp.get("utm")
                if isinstance(utm, dict):
                    if utm.get("source"):
                        source_field_path_counts["utm.source"] = source_field_path_counts.get("utm.source", 0) + 1
                    if utm.get("medium"):
                        medium_field_path_counts["utm.medium"] = medium_field_path_counts.get("utm.medium", 0) + 1
                    if utm.get("campaign"):
                        campaign_field_path_counts["utm.campaign"] = campaign_field_path_counts.get("utm.campaign", 0) + 1
                    raw_source = raw_source or _normalized_text(utm.get("source"))
                    raw_medium = raw_medium or _normalized_text(utm.get("medium"))
                    raw_campaign = raw_campaign or _normalized_text(utm.get("campaign"))
                source_obj = tp.get("source")
                if isinstance(source_obj, dict):
                    if source_obj.get("platform"):
                        source_field_path_counts["source.platform"] = source_field_path_counts.get("source.platform", 0) + 1
                    if source_obj.get("campaign_name"):
                        campaign_field_path_counts["source.campaign_name"] = campaign_field_path_counts.get("source.campaign_name", 0) + 1
                    raw_source = raw_source or _normalized_text(source_obj.get("platform"))
                    raw_campaign = raw_campaign or _normalized_text(source_obj.get("campaign_name"))
                campaign_obj = tp.get("campaign")
                if isinstance(campaign_obj, dict):
                    if campaign_obj.get("name"):
                        campaign_field_path_counts["campaign.name"] = campaign_field_path_counts.get("campaign.name", 0) + 1
                    raw_campaign = raw_campaign or _normalized_text(campaign_obj.get("name"))
                _inc(source_counts, raw_source)
                _inc(medium_counts, raw_medium)
                _inc(campaign_counts, raw_campaign)
                if raw_source or raw_medium:
                    pair_key = f"{raw_source or ''}||{raw_medium or ''}"
                    source_medium_pair_counts[pair_key] = source_medium_pair_counts.get(pair_key, 0) + 1

    return {
        "conversion_count": conversion_count,
        "touchpoint_count": touchpoint_count,
        "conversion_event_counts": conversion_event_counts,
        "channel_counts": channel_counts,
        "source_counts": source_counts,
        "medium_counts": medium_counts,
        "campaign_counts": campaign_counts,
        "source_medium_pair_counts": source_medium_pair_counts,
        "value_field_counts": value_field_counts,
        "currency_field_counts": currency_field_counts,
        "touchpoint_attr_counts": touchpoint_attr_counts,
        "channel_field_path_counts": channel_field_path_counts,
        "source_field_path_counts": source_field_path_counts,
        "medium_field_path_counts": medium_field_path_counts,
        "campaign_field_path_counts": campaign_field_path_counts,
        "dedup_key_counts": dedup_key_counts,
    }


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    get_base_url_fn: Callable[[], str],
    get_data_dir_obj: Callable[[], Path],
    get_datasets_obj: Callable[[], Dict[str, Any]],
    get_settings_obj: Callable[[], Any],
    meiro_parser_version: str,
    import_journeys_from_cdp_fn: Callable[..., Dict[str, Any]],
    from_cdp_request_factory: Callable[..., Any],
    attribution_mapping_config_cls: Any,
    canonicalize_meiro_profiles_fn: Callable[..., Dict[str, Any]],
    persist_journeys_fn: Callable[..., None],
    refresh_journey_aggregates_fn: Callable[..., None],
    append_import_run_fn: Callable[..., None],
    set_active_journey_source_fn: Callable[[str], None],
    set_journeys_cache_fn: Callable[[List[Dict[str, Any]]], None],
    get_journeys_revenue_value_fn: Callable[..., float],
) -> APIRouter:
    router = APIRouter(tags=["meiro_integration"])

    @router.post("/api/connectors/meiro/test")
    def meiro_test(req: MeiroCDPTestRequest = MeiroCDPTestRequest()):
        api_base_url = req.api_base_url
        api_key = req.api_key
        if not api_base_url or not api_key:
            cfg = meiro_cdp.get_config()
            if not cfg:
                raise HTTPException(status_code=400, detail="No saved credentials. Provide api_base_url and api_key.")
            api_base_url = cfg["api_base_url"]
            api_key = cfg["api_key"]
        result = meiro_cdp.test_connection(api_base_url, api_key)
        if result.get("ok"):
            if req.save_on_success:
                meiro_cdp.save_config(api_base_url, api_key)
            meiro_cdp.update_last_test_at()
            return {"ok": True, "message": "Connection successful"}
        raise HTTPException(status_code=400, detail=result.get("message", "Connection failed"))

    @router.post("/api/connectors/meiro/connect")
    def meiro_connect(req: MeiroCDPConnectRequest):
        result = meiro_cdp.test_connection(req.api_base_url, req.api_key)
        if result.get("ok"):
            meiro_cdp.save_config(req.api_base_url, req.api_key)
            meiro_cdp.update_last_test_at()
            return {"message": "Connected to Meiro CDP", "connected": True}
        raise HTTPException(status_code=400, detail=result.get("message", "Connection failed"))

    @router.post("/api/connectors/meiro/save")
    def meiro_save(req: MeiroCDPConnectRequest):
        meiro_cdp.save_config(req.api_base_url, req.api_key)
        return {"message": "Credentials saved"}

    @router.delete("/api/connectors/meiro")
    def meiro_disconnect():
        if meiro_cdp.disconnect():
            return {"message": "Disconnected from Meiro CDP"}
        raise HTTPException(status_code=404, detail="No Meiro CDP connection found")

    @router.get("/api/connectors/meiro/status")
    def meiro_status():
        return {"connected": meiro_cdp.is_connected()}

    @router.get("/api/connectors/meiro/config")
    def meiro_config():
        meta = meiro_cdp.get_connection_metadata()
        webhook_url = f"{get_base_url_fn()}/api/connectors/meiro/profiles"
        return {
            "connected": meiro_cdp.is_connected(),
            "api_base_url": meta["api_base_url"] if meta else None,
            "last_test_at": meta["last_test_at"] if meta else get_last_test_at(),
            "has_key": meta["has_key"] if meta else False,
            "webhook_url": webhook_url,
            "webhook_last_received_at": get_webhook_last_received_at(),
            "webhook_received_count": get_webhook_received_count(),
            "webhook_has_secret": bool(get_webhook_secret() or os.getenv("MEIRO_WEBHOOK_SECRET", "").strip()),
        }

    @router.get("/api/connectors/meiro/readiness")
    def meiro_readiness():
        config = meiro_config()
        mapping_state = get_mapping_state()
        archive_status = get_webhook_archive_status()
        pull_config = get_pull_config()
        return build_meiro_readiness(
            meiro_connected=meiro_cdp.is_connected(),
            meiro_config=config,
            mapping_state=mapping_state,
            archive_status=archive_status,
            pull_config=pull_config,
        )

    @router.get("/api/connectors/meiro/attributes")
    def meiro_attributes():
        if not meiro_cdp.is_connected():
            raise HTTPException(status_code=401, detail="Meiro CDP not connected")
        return meiro_cdp.list_attributes()

    @router.get("/api/connectors/meiro/events")
    def meiro_events():
        if not meiro_cdp.is_connected():
            raise HTTPException(status_code=401, detail="Meiro CDP not connected")
        return meiro_cdp.list_events()

    @router.get("/api/connectors/meiro/segments")
    def meiro_segments():
        if not meiro_cdp.is_connected():
            raise HTTPException(status_code=401, detail="Meiro CDP not connected")
        return meiro_cdp.list_segments()

    @router.post("/api/connectors/meiro/fetch")
    def meiro_fetch(req: MeiroCDPExportRequest):
        if not meiro_cdp.is_connected():
            raise HTTPException(status_code=401, detail="Meiro CDP not connected")
        try:
            df = meiro_cdp.fetch_and_transform(
                since=req.since,
                until=req.until,
                event_types=req.event_types,
                attributes=req.attributes,
                segment_id=req.segment_id,
            )
            out_path = get_data_dir_obj() / "meiro_cdp.csv"
            df.to_csv(out_path, index=False)
            get_datasets_obj()["meiro-cdp-export"] = {"path": out_path, "type": "sales"}
            return {"rows": len(df), "path": str(out_path), "dataset_id": "meiro-cdp-export"}
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    @router.get("/api/connectors/meiro/profiles")
    def meiro_profiles_status():
        out_path = get_data_dir_obj() / "meiro_cdp_profiles.json"
        count = 0
        if out_path.exists():
            try:
                data = json.loads(out_path.read_text())
                count = len(data) if isinstance(data, list) else 0
            except Exception:
                pass
        webhook_url = f"{get_base_url_fn()}/api/connectors/meiro/profiles"
        return {
            "stored_count": count,
            "webhook_url": webhook_url,
            "webhook_last_received_at": get_webhook_last_received_at(),
            "webhook_received_count": get_webhook_received_count(),
        }

    @router.post("/api/connectors/meiro/profiles")
    async def meiro_receive_profiles(
        request: Request,
        x_meiro_webhook_secret: Optional[str] = Header(None, alias="X-Meiro-Webhook-Secret"),
    ):
        webhook_secret = get_webhook_secret() or os.getenv("MEIRO_WEBHOOK_SECRET", "").strip()
        if webhook_secret and (not x_meiro_webhook_secret or x_meiro_webhook_secret != webhook_secret):
            raise HTTPException(status_code=401, detail="Invalid or missing X-Meiro-Webhook-Secret")

        try:
            body = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")

        if isinstance(body, list):
            profiles = body
            replace = True
        elif isinstance(body, dict):
            profiles = body.get("profiles")
            if not isinstance(profiles, list):
                data_payload = body.get("data")
                if isinstance(data_payload, list):
                    profiles = data_payload
                elif isinstance(data_payload, dict):
                    nested_profiles = data_payload.get("profiles")
                    profiles = nested_profiles if isinstance(nested_profiles, list) else None
            if not isinstance(profiles, list):
                journeys_payload = body.get("journeys")
                if isinstance(journeys_payload, list):
                    profiles = journeys_payload
            if not isinstance(profiles, list):
                profiles = []
            replace = body.get("replace", True)
            if not isinstance(profiles, list):
                raise HTTPException(
                    status_code=400,
                    detail="Body must be an array, { 'profiles': [...] }, { 'data': [...] }, or { 'journeys': [...] }",
                )
        else:
            raise HTTPException(status_code=400, detail="Body must be JSON array or object with 'profiles' key")

        out_path = get_data_dir_obj() / "meiro_cdp_profiles.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if replace or not out_path.exists():
            to_store = profiles
        else:
            try:
                existing = json.loads(out_path.read_text())
                to_store = (existing if isinstance(existing, list) else []) + list(profiles)
            except Exception:
                to_store = profiles

        out_path.write_text(json.dumps(to_store, indent=2))
        now_iso = datetime.utcnow().isoformat() + "Z"
        set_webhook_received(count_delta=len(profiles), last_received_at=now_iso)
        payload_excerpt, payload_truncated, payload_bytes = _safe_json_excerpt(body)
        conversion_names, channels_detected = _extract_payload_hints(profiles)
        payload_analysis = _analyze_payload(profiles)
        append_webhook_archive_entry(
            {
                "received_at": now_iso,
                "parser_version": meiro_parser_version,
                "replace": bool(replace),
                "payload_shape": "array" if isinstance(body, list) else "object",
                "received_count": int(len(profiles)),
                "profiles": profiles,
            }
        )
        append_webhook_event(
            {
                "received_at": now_iso,
                "received_count": int(len(profiles)),
                "stored_total": int(len(to_store)),
                "replace": bool(replace),
                "parser_version": meiro_parser_version,
                "ip": request.client.host if request.client else None,
                "user_agent": (request.headers.get("user-agent") or "")[:256] or None,
                "payload_shape": "array" if isinstance(body, list) else "object",
                "payload_excerpt": payload_excerpt,
                "payload_truncated": payload_truncated,
                "payload_bytes": payload_bytes,
                "payload_json_valid": True,
                "conversion_event_names": conversion_names,
                "channels_detected": channels_detected,
                "payload_analysis": payload_analysis,
            },
            max_items=100,
        )
        return {
            "received": len(profiles),
            "stored_total": len(to_store),
            "message": "Profiles saved. Use Import from CDP in Data Sources to load into attribution.",
        }

    @router.post("/api/connectors/meiro/webhook/rotate-secret")
    def meiro_webhook_rotate():
        secret = rotate_webhook_secret()
        return {"message": "Webhook secret rotated", "secret": secret}

    @router.get("/api/connectors/meiro/webhook/events")
    def meiro_webhook_events(
        limit: int = Query(100, ge=1, le=500),
        include_payload: bool = Query(False, description="Include payload excerpts for debugging"),
    ):
        events = get_webhook_events(limit=limit)
        if not include_payload:
            events = [{k: v for k, v in event.items() if k not in {"payload_excerpt"}} for event in events]
        return {"items": events, "total": len(events)}

    @router.get("/api/connectors/meiro/webhook/archive-status")
    def meiro_webhook_archive_status():
        return get_webhook_archive_status()

    @router.get("/api/connectors/meiro/webhook/archive")
    def meiro_webhook_archive(limit: int = Query(25, ge=1, le=500)):
        items = get_webhook_archive_entries(limit=limit)
        return {"items": items, "total": len(items)}

    @router.post("/api/connectors/meiro/webhook/reprocess")
    def meiro_webhook_reprocess(
        payload: MeiroWebhookReprocessRequest = Body(default=MeiroWebhookReprocessRequest()),
        db=Depends(get_db_dependency),
    ):
        archive_entries = get_webhook_archive_entries(limit=payload.archive_limit or 5000)
        archived_profiles = rebuild_profiles_from_webhook_archive(limit=payload.archive_limit)
        if not archived_profiles:
            raise HTTPException(status_code=404, detail="No archived webhook payloads found to reprocess.")
        mapping_state = get_mapping_state()

        out_path = get_data_dir_obj() / "meiro_cdp_profiles.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(archived_profiles, indent=2))

        result: Dict[str, Any] = {
            "reprocessed_profiles": len(archived_profiles),
            "archive_entries_used": len(archive_entries),
            "parser_version": meiro_parser_version,
            "mapping_version": mapping_state.get("version") or 0,
            "mapping_approval_status": ((mapping_state.get("approval") or {}).get("status") or "unreviewed"),
            "persisted_to_attribution": False,
        }
        if payload.persist_to_attribution:
            import_result = import_journeys_from_cdp_fn(
                req=from_cdp_request_factory(
                    config_id=payload.config_id,
                    import_note=payload.import_note or "Reprocessed from webhook archive",
                ),
                db=db,
            )
            result["persisted_to_attribution"] = True
            result["import_result"] = import_result
        return result

    @router.get("/api/connectors/meiro/webhook/suggestions")
    def meiro_webhook_suggestions(limit: int = Query(100, ge=1, le=500)):
        events = get_webhook_events(limit=limit)
        conversion_event_counts: Dict[str, int] = {}
        channel_counts: Dict[str, int] = {}
        source_counts: Dict[str, int] = {}
        medium_counts: Dict[str, int] = {}
        campaign_counts: Dict[str, int] = {}
        source_medium_pair_counts: Dict[str, int] = {}
        value_field_counts: Dict[str, int] = {}
        currency_field_counts: Dict[str, int] = {}
        touchpoint_attr_counts: Dict[str, int] = {}
        channel_field_path_counts: Dict[str, int] = {}
        source_field_path_counts: Dict[str, int] = {}
        medium_field_path_counts: Dict[str, int] = {}
        campaign_field_path_counts: Dict[str, int] = {}
        dedup_key_counts: Dict[str, int] = {"conversion_id": 0, "order_id": 0, "event_id": 0}
        total_conversions = 0
        total_touchpoints = 0

        def _merge_counts(target: Dict[str, int], incoming: Dict[str, Any]) -> None:
            for key, value in (incoming or {}).items():
                try:
                    count = int(value)
                except Exception:
                    continue
                if key:
                    target[key] = target.get(key, 0) + count

        for event in events:
            analysis = event.get("payload_analysis") if isinstance(event, dict) else None
            if not isinstance(analysis, dict):
                continue
            total_conversions += int(analysis.get("conversion_count") or 0)
            total_touchpoints += int(analysis.get("touchpoint_count") or 0)
            _merge_counts(conversion_event_counts, analysis.get("conversion_event_counts") or {})
            _merge_counts(channel_counts, analysis.get("channel_counts") or {})
            _merge_counts(source_counts, analysis.get("source_counts") or {})
            _merge_counts(medium_counts, analysis.get("medium_counts") or {})
            _merge_counts(campaign_counts, analysis.get("campaign_counts") or {})
            _merge_counts(source_medium_pair_counts, analysis.get("source_medium_pair_counts") or {})
            _merge_counts(value_field_counts, analysis.get("value_field_counts") or {})
            _merge_counts(currency_field_counts, analysis.get("currency_field_counts") or {})
            _merge_counts(touchpoint_attr_counts, analysis.get("touchpoint_attr_counts") or {})
            _merge_counts(channel_field_path_counts, analysis.get("channel_field_path_counts") or {})
            _merge_counts(source_field_path_counts, analysis.get("source_field_path_counts") or {})
            _merge_counts(medium_field_path_counts, analysis.get("medium_field_path_counts") or {})
            _merge_counts(campaign_field_path_counts, analysis.get("campaign_field_path_counts") or {})
            for key in ("conversion_id", "order_id", "event_id"):
                dedup_key_counts[key] = dedup_key_counts.get(key, 0) + int((analysis.get("dedup_key_counts") or {}).get(key, 0) or 0)

        def _top_items(values: Dict[str, int], n: int = 10) -> List[Tuple[str, int]]:
            return sorted(values.items(), key=lambda kv: kv[1], reverse=True)[:n]

        top_conversion_names = _top_items(conversion_event_counts, n=10)
        top_value_fields = _top_items(value_field_counts, n=5)
        top_currency_fields = _top_items(currency_field_counts, n=5)
        top_sources = _top_items(source_counts, n=15)
        top_mediums = _top_items(medium_counts, n=15)
        top_campaigns = _top_items(campaign_counts, n=15)
        top_source_medium_pairs = _top_items(source_medium_pair_counts, n=20)
        top_touchpoint_attrs = _top_items(touchpoint_attr_counts, n=5)
        top_channel_field_paths = _top_items(channel_field_path_counts, n=5)
        top_source_field_paths = _top_items(source_field_path_counts, n=5)
        top_medium_field_paths = _top_items(medium_field_path_counts, n=5)
        top_campaign_field_paths = _top_items(campaign_field_path_counts, n=5)
        best_dedup_key = sorted(dedup_key_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]

        dedup_key_candidates = []
        for key, count in sorted(dedup_key_counts.items(), key=lambda kv: kv[1], reverse=True):
            coverage = (count / total_conversions) if total_conversions > 0 else 0.0
            dedup_key_candidates.append(
                {
                    "key": key,
                    "count": count,
                    "coverage_pct": round(coverage * 100, 2),
                    "recommended": key == best_dedup_key,
                }
            )

        kpi_suggestions = []
        for idx, (event_name, count) in enumerate(top_conversion_names):
            if not event_name:
                continue
            event_id = re.sub(r"[^a-z0-9_]+", "_", event_name.lower()).strip("_")[:64] or f"event_{idx+1}"
            coverage = (count / total_conversions) if total_conversions > 0 else 0.0
            value_field = top_value_fields[0][0] if top_value_fields else None
            kpi_suggestions.append(
                {
                    "id": event_id,
                    "label": event_name.replace("_", " ").title(),
                    "type": "primary" if idx == 0 else "micro",
                    "event_name": event_name,
                    "value_field": value_field if idx == 0 else None,
                    "weight": 1.0 if idx == 0 else 0.5,
                    "lookback_days": 30 if idx == 0 else 14,
                    "coverage_pct": round(coverage * 100, 2),
                }
            )

        primary_kpi_id = kpi_suggestions[0]["id"] if kpi_suggestions else None
        current_taxonomy = load_taxonomy()

        def _normalized_label(value: Optional[str]) -> str:
            return str(value or "").strip().lower()

        def _suggest_source_alias(raw_source: str) -> Optional[str]:
            known = {
                "fb": "facebook",
                "ig": "instagram",
                "yt": "youtube",
                "li": "linkedin",
                "tt": "tiktok",
                "gads": "google",
                "adwords": "google",
            }
            return known.get(_normalized_label(raw_source))

        def _suggest_medium_alias(raw_medium: str) -> Optional[str]:
            known = {
                "ppc": "cpc",
                "paid": "cpc",
                "paidsearch": "cpc",
                "paid_search": "cpc",
                "paidsocial": "paid_social",
                "paid-social": "paid_social",
                "social_paid": "paid_social",
            }
            return known.get(_normalized_label(raw_medium))

        def _classify_channel(source: str, medium: str) -> Optional[str]:
            src = _normalized_label(source)
            med = _normalized_label(medium)
            if med in {"email", "e-mail"} or "email" in med:
                return "email"
            if med in {"(none)", "none", "direct", ""} and src in {"", "direct"}:
                return "direct"
            if med in {"cpc", "ppc", "paid_search"} and re.search(r"google|bing|baidu|adwords", src):
                return "paid_search"
            if med in {"paid_social", "social", "social_paid", "paid"} and re.search(r"facebook|meta|instagram|linkedin|twitter|x|tiktok", src):
                return "paid_social"
            if src in {"newsletter", "mailchimp", "klaviyo", "braze", "customerio"}:
                return "email"
            return None

        def _matches_existing_rule(source: str, medium: str, campaign: str = "") -> bool:
            source_norm = current_taxonomy.source_aliases.get(source, source)
            medium_norm = current_taxonomy.medium_aliases.get(medium, medium)
            for rule in current_taxonomy.channel_rules:
                if rule.matches(source_norm, medium_norm, campaign):
                    return True
            return False

        source_aliases = dict(current_taxonomy.source_aliases)
        suggested_source_aliases: Dict[str, str] = {}
        for source, _count in top_sources:
            lower = _normalized_label(source)
            if not lower or lower in source_aliases:
                continue
            mapped = _suggest_source_alias(lower)
            if mapped and mapped != lower:
                source_aliases[lower] = mapped
                suggested_source_aliases[lower] = mapped

        medium_aliases = dict(current_taxonomy.medium_aliases)
        suggested_medium_aliases: Dict[str, str] = {}
        for medium, _count in top_mediums:
            lower = _normalized_label(medium)
            if not lower or lower in medium_aliases:
                continue
            mapped = _suggest_medium_alias(lower)
            if mapped and mapped != lower:
                medium_aliases[lower] = mapped
                suggested_medium_aliases[lower] = mapped

        taxonomy_rules = []
        unresolved_pairs = []
        seen_rule_keys = {
            (
                rule.channel,
                rule.source.normalize_operator(),
                rule.source.value,
                rule.medium.normalize_operator(),
                rule.medium.value,
                rule.campaign.normalize_operator(),
                rule.campaign.value,
            )
            for rule in current_taxonomy.channel_rules
        }
        priority = max([rule.priority for rule in current_taxonomy.channel_rules] or [0]) + 10
        for pair_key, count in top_source_medium_pairs:
            raw_source, raw_medium = (pair_key.split("||", 1) + [""])[:2]
            source_norm = source_aliases.get(raw_source, raw_source)
            medium_norm = medium_aliases.get(raw_medium, raw_medium)
            if not source_norm and not medium_norm:
                continue
            if _matches_existing_rule(source_norm, medium_norm):
                continue
            suggested_channel = _classify_channel(source_norm, medium_norm)
            if suggested_channel:
                active_conditions = int(bool(source_norm)) + int(bool(medium_norm))
                if active_conditions == 0:
                    continue
                rule_key = (suggested_channel, "equals", source_norm, "equals", medium_norm, "any", "")
                if rule_key in seen_rule_keys:
                    continue
                taxonomy_rules.append(
                    {
                        "name": f"Auto: {suggested_channel} from {source_norm or 'source'} / {medium_norm or 'medium'}",
                        "channel": suggested_channel,
                        "priority": priority,
                        "enabled": True,
                        "source": {"operator": "equals" if source_norm else "any", "value": source_norm},
                        "medium": {"operator": "equals" if medium_norm else "any", "value": medium_norm},
                        "campaign": {"operator": "any", "value": ""},
                        "observed_count": count,
                        "match_source": source_norm,
                        "match_medium": medium_norm,
                    }
                )
                seen_rule_keys.add(rule_key)
                priority += 10
            else:
                unresolved_pairs.append({"source": source_norm, "medium": medium_norm, "count": count})
            if len(taxonomy_rules) >= 12:
                break

        kpi_apply_payload = {
            "definitions": [
                {
                    "id": item["id"],
                    "label": item["label"],
                    "type": item["type"],
                    "event_name": item["event_name"],
                    "value_field": item["value_field"],
                    "weight": item["weight"],
                    "lookback_days": item["lookback_days"],
                }
                for item in kpi_suggestions
            ],
            "primary_kpi_id": primary_kpi_id,
        }
        taxonomy_apply_payload = {
            "channel_rules": [
                {
                    "name": rule.name,
                    "channel": rule.channel,
                    "priority": rule.priority,
                    "enabled": rule.enabled,
                    "source": {"operator": rule.source.normalize_operator(), "value": rule.source.value or ""},
                    "medium": {"operator": rule.medium.normalize_operator(), "value": rule.medium.value or ""},
                    "campaign": {"operator": rule.campaign.normalize_operator(), "value": rule.campaign.value or ""},
                }
                for rule in current_taxonomy.channel_rules
            ]
            + [{k: v for k, v in rule.items() if k not in {"observed_count", "match_source", "match_medium"}} for rule in taxonomy_rules],
            "source_aliases": source_aliases,
            "medium_aliases": medium_aliases,
        }
        mapping_apply_payload = {
            "touchpoint_attr": top_touchpoint_attrs[0][0] if top_touchpoint_attrs else "touchpoints",
            "value_attr": top_value_fields[0][0] if top_value_fields else "conversion_value",
            "id_attr": "customer_id",
            "channel_field": top_channel_field_paths[0][0] if top_channel_field_paths else "channel",
            "timestamp_field": "timestamp",
            "source_field": top_source_field_paths[0][0] if top_source_field_paths else "source",
            "medium_field": top_medium_field_paths[0][0] if top_medium_field_paths else "medium",
            "campaign_field": top_campaign_field_paths[0][0] if top_campaign_field_paths else "campaign",
            "currency_field": top_currency_fields[0][0] if top_currency_fields else "currency",
        }

        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "events_analyzed": len(events),
            "total_conversions_observed": total_conversions,
            "total_touchpoints_observed": total_touchpoints,
            "dedup_key_suggestion": best_dedup_key,
            "dedup_key_candidates": dedup_key_candidates,
            "kpi_suggestions": kpi_suggestions,
            "conversion_event_suggestions": [{"event_name": name, "count": count} for name, count in top_conversion_names],
            "taxonomy_suggestions": {
                "channel_rules": taxonomy_rules,
                "source_aliases": suggested_source_aliases,
                "medium_aliases": suggested_medium_aliases,
                "top_sources": [{"source": name, "count": count} for name, count in top_sources],
                "top_mediums": [{"medium": name, "count": count} for name, count in top_mediums],
                "top_campaigns": [{"campaign": name, "count": count} for name, count in _top_items(campaign_counts, n=15)],
                "observed_pairs": [
                    {
                        "source": (name.split("||", 1) + [""])[0],
                        "medium": (name.split("||", 1) + [""])[1],
                        "count": count,
                    }
                    for name, count in top_source_medium_pairs
                ],
                "unresolved_pairs": unresolved_pairs[:10],
            },
            "mapping_suggestions": {
                "touchpoint_attr_candidates": [{"path": name, "count": count} for name, count in top_touchpoint_attrs],
                "value_field_candidates": [{"path": name, "count": count} for name, count in top_value_fields],
                "currency_field_candidates": [{"path": name, "count": count} for name, count in top_currency_fields],
                "channel_field_candidates": [{"path": name, "count": count} for name, count in top_channel_field_paths],
                "source_field_candidates": [{"path": name, "count": count} for name, count in top_source_field_paths],
                "medium_field_candidates": [{"path": name, "count": count} for name, count in top_medium_field_paths],
                "campaign_field_candidates": [{"path": name, "count": count} for name, count in top_campaign_field_paths],
            },
            "apply_payloads": {
                "kpis": kpi_apply_payload,
                "taxonomy": taxonomy_apply_payload,
                "mapping": mapping_apply_payload,
            },
        }

    @router.get("/api/connectors/meiro/mapping")
    def meiro_get_mapping():
        return {**get_mapping_state(), "presets": MEIRO_MAPPING_PRESETS}

    @router.post("/api/connectors/meiro/mapping")
    def meiro_save_mapping(mapping: dict):
        save_mapping(mapping)
        return {"message": "Mapping saved", **get_mapping_state()}

    @router.post("/api/connectors/meiro/mapping/approval")
    def meiro_update_mapping_approval(payload: MeiroMappingApprovalRequest):
        state = update_mapping_approval(payload.status, payload.note)
        return {"message": "Mapping approval updated", **state}

    @router.get("/api/connectors/meiro/pull-config")
    def meiro_get_pull_config():
        return get_pull_config()

    @router.post("/api/connectors/meiro/pull-config")
    def meiro_save_pull_config(config: dict):
        save_pull_config(config)
        return {"message": "Pull config saved"}

    @router.post("/api/connectors/meiro/pull")
    def meiro_pull(since: Optional[str] = None, until: Optional[str] = None, db=Depends(get_db_dependency)):
        if not meiro_cdp.is_connected():
            raise HTTPException(status_code=401, detail="Meiro CDP not connected")
        today = datetime.utcnow().date()
        pull_cfg = get_pull_config()
        lookback = pull_cfg.get("lookback_days", 30)
        start = (today - timedelta(days=lookback)).isoformat()
        end = today.isoformat()
        since = since or start
        until = until or end
        saved = get_mapping()
        try:
            records = meiro_cdp.fetch_raw_events(since=since, until=until)
        except Exception as exc:
            append_import_run_fn("meiro_pull", 0, "error", error=str(exc))
            raise HTTPException(status_code=500, detail=str(exc))
        journeys = meiro_cdp.build_journeys_from_events(
            records,
            id_attr=saved.get("id_attr", "customer_id"),
            timestamp_attr=saved.get("timestamp_field", "timestamp"),
            channel_attr=saved.get("channel_field", "channel"),
            conversion_selector=pull_cfg.get("conversion_selector", "purchase"),
            session_gap_minutes=pull_cfg.get("session_gap_minutes", 30),
            dedup_interval_minutes=pull_cfg.get("dedup_interval_minutes", 5),
            dedup_mode=pull_cfg.get("dedup_mode", "balanced"),
            channel_mapping=saved.get("channel_mapping"),
        )
        set_journeys_cache_fn(journeys)
        persist_journeys_fn(db, journeys, replace=True)
        refresh_journey_aggregates_fn(db)
        converted = sum(1 for journey in journeys if journey.get("converted", True))
        channels_detected = sorted(
            {
                touchpoint.get("channel", "unknown")
                for journey in journeys
                for touchpoint in journey.get("touchpoints", [])
            }
        )
        append_import_run_fn(
            "meiro_pull",
            len(journeys),
            "success",
            total=len(journeys),
            valid=len(journeys),
            converted=converted,
            channels_detected=channels_detected,
            config_snapshot={
                "lookback_days": pull_cfg.get("lookback_days"),
                "session_gap_minutes": pull_cfg.get("session_gap_minutes"),
                "conversion_selector": pull_cfg.get("conversion_selector"),
                "dedup_interval_minutes": pull_cfg.get("dedup_interval_minutes"),
                "dedup_mode": pull_cfg.get("dedup_mode"),
                "primary_dedup_key": pull_cfg.get("primary_dedup_key"),
                "fallback_dedup_keys": pull_cfg.get("fallback_dedup_keys"),
            },
            preview_rows=[
                {
                    "customer_id": journey.get("customer_id", "?"),
                    "touchpoints": len(journey.get("touchpoints", [])),
                    "value": get_journeys_revenue_value_fn(journey),
                }
                for journey in journeys[:20]
            ],
        )
        set_active_journey_source_fn("meiro")
        return {"count": len(journeys), "message": f"Pulled {len(journeys)} journeys from Meiro"}

    @router.post("/api/connectors/meiro/dry-run")
    def meiro_dry_run(limit: int = 100):
        data_dir = get_data_dir_obj()
        cdp_json_path = data_dir / "meiro_cdp_profiles.json"
        cdp_path = data_dir / "meiro_cdp.csv"
        saved = get_mapping()
        mapping = attribution_mapping_config_cls(
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
        if cdp_json_path.exists():
            profiles = json.loads(cdp_json_path.read_text())
        elif cdp_path.exists():
            df = pd.read_csv(cdp_path)
            profiles = df.to_dict(orient="records")
        else:
            raise HTTPException(status_code=404, detail="No CDP data found. Fetch or push profiles first.")
        profiles = profiles[:limit] if isinstance(profiles, list) else []
        if not profiles:
            return {"count": 0, "preview": [], "warnings": ["No profiles to process"], "validation": {}}
        try:
            pull_cfg = get_pull_config()
            settings = get_settings_obj()
            revenue_config = settings.revenue_config
            result = canonicalize_meiro_profiles_fn(
                profiles,
                mapping=mapping.model_dump(),
                revenue_config=(revenue_config.model_dump() if hasattr(revenue_config, "model_dump") else revenue_config),
                dedup_config=pull_cfg,
            )
            journeys = result["valid_journeys"]
        except Exception as exc:
            return {"count": 0, "preview": [], "warnings": [str(exc)], "validation": {"error": str(exc)}}
        warnings = []
        if journeys:
            sample_channels = {
                touchpoint.get("channel")
                for journey in journeys
                for touchpoint in journey.get("touchpoints", [])
                if isinstance(touchpoint, dict) and touchpoint.get("channel")
            }
            if not any(channel for channel in sample_channels if "email" in str(channel).lower() or "click" in str(channel).lower()):
                warnings.append("No email click tracking detected; channel coverage may be incomplete")
        preview = [
            {
                "id": journey.get("customer_id", journey.get("id", "?")),
                "touchpoints": len(journey.get("touchpoints", [])),
                "value": get_journeys_revenue_value_fn(journey),
            }
            for journey in journeys[:20]
        ]
        return {"count": len(journeys), "preview": preview, "warnings": warnings, "validation": {"ok": len(warnings) == 0}}

    return router
