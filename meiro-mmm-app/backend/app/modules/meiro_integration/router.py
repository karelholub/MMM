import json
import logging
import os
import re
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError

from app import services_meiro_api
from app.connectors import meiro_cdp
from app.modules.meiro_integration.schemas import (
    MeiroCDPConnectRequest,
    MeiroCDPExportRequest,
    MeiroCDPTestRequest,
    MeiroMappingApprovalRequest,
    MeiroWebhookReprocessRequest,
)
from app.utils.meiro_config import (
    append_event_archive_entry,
    append_auto_replay_history,
    append_webhook_archive_entry,
    archive_source_metadata,
    append_webhook_event,
    get_event_archive_status,
    get_last_test_at,
    get_target_instance_host,
    get_target_instance_url,
    get_mapping,
    get_mapping_state,
    get_auto_replay_state,
    get_auto_replay_history,
    get_pull_config,
    query_webhook_archive_entries,
    get_webhook_archive_status,
    get_webhook_events,
    get_webhook_last_received_at,
    get_webhook_received_count,
    get_webhook_secret,
    query_event_archive_entries,
    rebuild_profiles_from_webhook_archive,
    rotate_webhook_secret,
    save_mapping,
    save_pull_config,
    set_webhook_received,
    instance_scope,
    update_auto_replay_state,
    update_mapping_approval,
)
from app.services_meiro_readiness import build_meiro_readiness
from app.services_meiro_event_contract import build_event_contract_readiness, build_sample_contract_events
from app.services_meiro_quarantine import get_quarantine_run, get_quarantine_runs
from app.services_meiro_raw_batches import (
    MeiroRawBatchUnavailableError,
    get_meiro_raw_batch_status,
    list_meiro_raw_batches,
    rebuild_profiles_from_meiro_profile_batches,
)
from app.services_meiro_event_profile_state import (
    MeiroEventProfileStateUnavailableError,
    list_meiro_event_profile_state,
    upsert_meiro_event_profile_state,
)
from app.services_meiro_event_facts import (
    MeiroEventFactsUnavailableError,
    list_meiro_event_facts,
    upsert_meiro_event_facts,
)
from app.services_meiro_profile_facts import list_meiro_profile_facts, upsert_meiro_profile_facts
from app.services_meiro_replay_runs import list_meiro_replay_runs, record_meiro_replay_run
from app.services_meiro_replay_snapshots import create_meiro_replay_snapshot
from app.services_meiro_api import MeiroApiError
from app.utils.taxonomy import load_taxonomy

logger = logging.getLogger(__name__)


def _is_retryable_sqlite_operational_error(exc: Exception) -> bool:
    message = str(getattr(exc, "orig", exc) or exc).lower()
    retryable_markers = (
        "disk i/o error",
        "database is locked",
        "database table is locked",
        "unable to open database file",
        "readonly database",
        "cannot commit transaction",
    )
    return any(marker in message for marker in retryable_markers)


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


def _event_payload_with_outer_fields(item: Any) -> Dict[str, Any]:
    if not isinstance(item, dict):
        return {}
    nested = item.get("event_payload")
    if not isinstance(nested, dict):
        return item
    merged = dict(nested)
    for key, value in item.items():
        if key != "event_payload" and key not in merged:
            merged[key] = value
    return merged


def _extract_event_payload_hints(payload_events: list[Any]) -> tuple[list[str], list[str]]:
    event_names: set[str] = set()
    channels: set[str] = set()
    for item in payload_events[:200]:
        event = _event_payload_with_outer_fields(item)
        if not isinstance(event, dict):
            continue
        event_name = event.get("event_name") or event.get("event_type") or event.get("name") or event.get("type")
        if isinstance(event_name, str) and event_name.strip():
            event_names.add(event_name.strip())
        channel = event.get("channel") or event.get("source") or event.get("utm_source")
        if isinstance(channel, str) and channel.strip():
            channels.add(channel.strip())
    return sorted(event_names)[:20], sorted(channels)[:20]


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
                _inc(
                    conversion_event_counts,
                    str(
                        conversion.get("name")
                        or conversion.get("event_name")
                        or conversion.get("type")
                        or ""
                    ).strip().lower(),
                )
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


_UUID_LIKE_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.IGNORECASE)


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalized_event_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _looks_like_meaningful_event_name(value: Any) -> bool:
    token = _normalized_event_token(value)
    if not token or token in {"other", "unknown", "unknown_event", "event"}:
        return False
    if _UUID_LIKE_RE.match(str(value or "").strip()):
        return False
    return True


def _looks_like_touchpoint_name(value: Any) -> bool:
    token = _normalized_event_token(value)
    if not token:
        return False
    decision_engine_touchpoints = {"inapp_message", "decision_action", "eligibility_check", "personalize"}
    return token in decision_engine_touchpoints or any(
        fragment in token for fragment in ("page_view", "session", "click", "impression", "visit", "landing", "view")
    )


def _looks_like_conversion_name(value: Any) -> bool:
    token = _normalized_event_token(value)
    if not token:
        return False
    return any(fragment in token for fragment in ("purchase", "checkout", "order", "lead", "submit", "signup", "refund", "cancel", "invalid", "disqual"))


def _build_raw_event_stream_diagnostics(*, event_archive_entries: list[Dict[str, Any]], webhook_events: list[Dict[str, Any]]) -> Dict[str, Any]:
    total_events = 0
    batches = 0
    usable_event_name_count = 0
    identity_count = 0
    source_medium_count = 0
    referrer_only_count = 0
    touchpoint_like_count = 0
    conversion_like_count = 0
    conversion_linkage_count = 0
    per_batch_profile_counts: list[float] = []

    for entry in event_archive_entries:
        if not isinstance(entry, dict):
            continue
        batches += 1
        for item in entry.get("events") or []:
            event = _event_payload_with_outer_fields(item)
            if not isinstance(event, dict):
                continue
            total_events += 1
            event_name = event.get("event_name") or event.get("event_type") or event.get("name") or event.get("type")
            if _looks_like_meaningful_event_name(event_name):
                usable_event_name_count += 1
            if event.get("customer_id") or event.get("profile_id") or event.get("meiro_profile_id") or event.get("user_id"):
                identity_count += 1
            has_source_medium = bool((event.get("source") or event.get("utm_source")) and (event.get("medium") or event.get("utm_medium")))
            has_referrer = bool(event.get("page_referrer") or event.get("referrer"))
            if has_source_medium:
                source_medium_count += 1
            elif has_referrer:
                referrer_only_count += 1
            if _looks_like_touchpoint_name(event_name):
                touchpoint_like_count += 1
            if _looks_like_conversion_name(event_name):
                conversion_like_count += 1
                if event.get("conversion_id") or event.get("order_id") or event.get("lead_id") or event.get("original_conversion_id"):
                    conversion_linkage_count += 1

    for event in webhook_events:
        if not isinstance(event, dict) or str(event.get("ingest_kind") or "") != "events":
            continue
        try:
            received = float(event.get("received_count") or 0)
            rebuilt = float(event.get("reconstructed_profiles") or 0)
        except Exception:
            continue
        if received > 0:
            per_batch_profile_counts.append(rebuilt / received)

    def _share(count: int, total: int) -> float:
        return round((count / total), 4) if total > 0 else 0.0

    warnings: list[str] = []
    usable_event_name_share = _share(usable_event_name_count, total_events)
    source_medium_share = _share(source_medium_count, total_events)
    referrer_only_share = _share(referrer_only_count, total_events)
    conversion_linkage_share = _share(conversion_linkage_count, conversion_like_count)
    if total_events > 0 and usable_event_name_share < 0.6:
        warnings.append("Many raw events still use generic or opaque event names.")
    if total_events > 0 and source_medium_share < 0.3 and referrer_only_share > 0.1:
        warnings.append("A large share of raw events relies on referrer fallback instead of explicit source/medium.")
    if conversion_like_count > 0 and conversion_linkage_share < 0.6:
        warnings.append("Many conversion-like raw events still lack stable order/lead/conversion linkage keys.")

    return {
        "available": total_events > 0,
        "batches_examined": batches,
        "events_examined": total_events,
        "usable_event_name_share": usable_event_name_share,
        "identity_share": _share(identity_count, total_events),
        "source_medium_share": source_medium_share,
        "referrer_only_share": referrer_only_share,
        "touchpoint_like_events": touchpoint_like_count,
        "conversion_like_events": conversion_like_count,
        "conversion_linkage_share": conversion_linkage_share,
        "avg_reconstructed_profiles_per_event": round(statistics.mean(per_batch_profile_counts), 4) if per_batch_profile_counts else 0.0,
        "warnings": warnings,
    }


def _build_event_replay_reconstruction_diagnostics(
    *,
    archive_entries: list[Dict[str, Any]],
    archived_profiles: list[Dict[str, Any]],
    import_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    events_loaded = sum(len(entry.get("events") or []) for entry in archive_entries if isinstance(entry, dict))
    touchpoints_reconstructed = 0
    conversions_reconstructed = 0
    attributable_profiles = 0
    profiles_with_touchpoints = 0
    profiles_with_conversions = 0

    for profile in archived_profiles:
        if not isinstance(profile, dict):
            continue
        touchpoints = profile.get("touchpoints") or []
        conversions = profile.get("conversions") or []
        touchpoint_count = len(touchpoints) if isinstance(touchpoints, list) else 0
        conversion_count = len(conversions) if isinstance(conversions, list) else 0
        touchpoints_reconstructed += touchpoint_count
        conversions_reconstructed += conversion_count
        if touchpoint_count > 0:
            profiles_with_touchpoints += 1
        if conversion_count > 0:
            profiles_with_conversions += 1
        if touchpoint_count > 0 and conversion_count > 0:
            attributable_profiles += 1

    diagnostics: Dict[str, Any] = {
        "archive_source": "events",
        "events_loaded": int(events_loaded),
        "profiles_reconstructed": int(len(archived_profiles)),
        "avg_events_per_profile": round((events_loaded / len(archived_profiles)), 2) if archived_profiles else 0.0,
        "touchpoints_reconstructed": int(touchpoints_reconstructed),
        "conversions_reconstructed": int(conversions_reconstructed),
        "profiles_with_touchpoints": int(profiles_with_touchpoints),
        "profiles_with_conversions": int(profiles_with_conversions),
        "attributable_profiles": int(attributable_profiles),
        "avg_touchpoints_per_profile": round((touchpoints_reconstructed / len(archived_profiles)), 2) if archived_profiles else 0.0,
        "avg_conversions_per_profile": round((conversions_reconstructed / len(archived_profiles)), 2) if archived_profiles else 0.0,
        "warnings": [],
    }

    warnings: list[str] = []
    if events_loaded > 0 and len(archived_profiles) == 0:
        warnings.append("Raw events were received, but none could be reconstructed into profiles.")
    if len(archived_profiles) > 0 and touchpoints_reconstructed == 0:
        warnings.append("Reconstructed profiles contain no touchpoints, so channel and campaign attribution will stay empty.")
    if len(archived_profiles) > 0 and conversions_reconstructed == 0:
        warnings.append("Reconstructed profiles contain no conversions, so attribution models have nothing to score.")
    if len(archived_profiles) > 0 and attributable_profiles == 0 and touchpoints_reconstructed > 0 and conversions_reconstructed > 0:
        warnings.append("Touchpoints and conversions exist, but not on the same reconstructed profiles.")

    if import_result:
        summary = (import_result or {}).get("import_summary") or {}
        cleaning_report = summary.get("cleaning_report") or {}
        journeys_persisted = int(import_result.get("count", 0) or 0)
        persisted_profile_count = int(import_result.get("persisted_profile_count", journeys_persisted) or 0)
        persisted_attributable_profile_count = int(
            import_result.get("persisted_attributable_profile_count", min(persisted_profile_count, attributable_profiles)) or 0
        )
        journeys_valid = int(summary.get("valid", journeys_persisted) or 0)
        journeys_quarantined = int(summary.get("quarantined", import_result.get("quarantine_count", 0)) or 0)
        journeys_invalid = int(summary.get("invalid", 0) or 0)
        journeys_converted = int(summary.get("converted", 0) or 0)
        diagnostics.update(
            {
                "journeys_valid": journeys_valid,
                "journeys_quarantined": journeys_quarantined,
                "journeys_invalid": journeys_invalid,
                "journeys_persisted": journeys_persisted,
                "persisted_profiles": persisted_profile_count,
                "persisted_attributable_profiles": persisted_attributable_profile_count,
                "journeys_converted": journeys_converted,
                "persisted_from_attributable_share": round((persisted_attributable_profile_count / attributable_profiles), 4)
                if attributable_profiles > 0
                else 0.0,
            }
        )
        if attributable_profiles > 0 and journeys_persisted == 0:
            warnings.append("Attributable reconstructed profiles were found, but none survived import into persisted journeys.")
        if journeys_quarantined > 0:
            warnings.append(f"{journeys_quarantined} reconstructed journeys were quarantined during import.")
        top_unresolved = cleaning_report.get("top_unresolved_patterns") or []
        if top_unresolved:
            top_label = ", ".join(
                f"{item.get('code', 'unknown')} ({int(item.get('count', 0) or 0)})"
                for item in top_unresolved[:3]
                if isinstance(item, dict)
            )
            if top_label:
                warnings.append(f"Top unresolved replay issues: {top_label}.")

    diagnostics["warnings"] = warnings
    return diagnostics


def _record_webhook_diagnostic_event(
    *,
    request: Request,
    parser_version: Optional[str] = None,
    ingest_kind: Optional[str] = None,
    payload_shape: str,
    status_code: int,
    outcome: str,
    error_class: Optional[str] = None,
    error_detail: Optional[str] = None,
    received_count: int = 0,
    stored_total: Optional[int] = None,
) -> None:
    now_iso = datetime.utcnow().isoformat() + "Z"
    append_webhook_event(
        {
            "received_at": now_iso,
            "received_count": int(received_count),
            "stored_total": int(stored_total or 0),
            "replace": False,
            "parser_version": parser_version,
            "ingest_kind": ingest_kind,
            "ip": request.client.host if request.client else None,
            "user_agent": (request.headers.get("user-agent") or "")[:256] or None,
            "payload_shape": payload_shape,
            "payload_json_valid": payload_shape != "invalid_json",
            "status_code": int(status_code),
            "outcome": outcome,
            "error_class": error_class,
            "error_detail": (error_detail or "")[:500] or None,
        },
        max_items=250,
    )


def _prefer_event_archive(
    requested_source: str,
    profile_archive_status: Dict[str, Any],
    event_archive_status: Dict[str, Any],
    *,
    primary_source: str = "profiles",
) -> bool:
    source = str(requested_source or "auto").strip().lower()
    if source == "events":
        return True
    if source == "profiles":
        return False
    profile_last = str(profile_archive_status.get("last_received_at") or "")
    event_last = str(event_archive_status.get("last_received_at") or "")
    profile_available = bool(profile_archive_status.get("available"))
    event_available = bool(event_archive_status.get("available"))
    preferred = str(primary_source or "profiles").strip().lower()
    if preferred == "events" and event_available:
        return True
    if preferred == "profiles" and profile_available:
        return False
    if event_available and not profile_available:
        return True
    if profile_available and not event_available:
        return False
    if event_available and profile_available:
        return event_last >= profile_last
    return False


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
    rebuild_profiles_from_meiro_events_fn: Callable[..., List[Dict[str, Any]]],
    extract_customer_ids_from_meiro_events_fn: Callable[..., List[str]],
    persist_journeys_fn: Callable[..., None],
    refresh_journey_aggregates_fn: Callable[..., None],
    append_import_run_fn: Callable[..., None],
    set_active_journey_source_fn: Callable[[str], None],
    set_journeys_cache_fn: Callable[[List[Dict[str, Any]]], None],
    get_journeys_revenue_value_fn: Callable[..., float],
    record_meiro_raw_batch_fn: Callable[..., Any],
    register_auto_replay_runner_fn: Optional[Callable[[Callable[..., Dict[str, Any]]], None]] = None,
) -> APIRouter:
    router = APIRouter(tags=["meiro_integration"])

    def _handle_meiro_api_error(exc: MeiroApiError) -> None:
        raise HTTPException(status_code=exc.status_code, detail=exc.to_response())

    def _build_saved_mapping_config() -> Dict[str, Any]:
        saved = get_mapping()
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
        return base.model_dump() if hasattr(base, "model_dump") else dict(base)

    def _get_profile_archive_status(db: Any) -> Dict[str, Any]:
        status = get_meiro_raw_batch_status(db, source_kind="profiles")
        return status if status.get("available") else get_webhook_archive_status()

    def _get_event_archive_status(db: Any) -> Dict[str, Any]:
        file_status = get_event_archive_status()
        db_status = get_meiro_raw_batch_status(db, source_kind="events")
        if file_status.get("available"):
            merged = dict(file_status)
            latest_batch_db_id = db_status.get("latest_batch_db_id")
            if latest_batch_db_id is not None:
                merged["latest_batch_db_id"] = latest_batch_db_id
            return merged
        return db_status if db_status.get("available") else file_status

    def _get_profile_archive_entries(
        db: Any,
        *,
        limit: Optional[int] = 100,
        after_db_id: Optional[int] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        items = list_meiro_raw_batches(
            db,
            source_kind="profiles",
            limit=limit,
            after_db_id=after_db_id,
            since=since,
            until=until,
        )
        if items:
            return items
        return query_webhook_archive_entries(limit=limit, since=since, until=until)

    def _get_event_archive_entries(
        db: Any,
        *,
        limit: Optional[int] = 100,
        after_db_id: Optional[int] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        items = list_meiro_raw_batches(
            db,
            source_kind="events",
            limit=limit,
            after_db_id=after_db_id,
            since=since,
            until=until,
        )
        if items:
            return items
        return query_event_archive_entries(limit=limit, since=since, until=until)

    def _get_event_archive_entries_for_replay(
        db: Any,
        *,
        limit: Optional[int] = 100,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        file_items = query_event_archive_entries(limit=limit, since=since, until=until)
        if file_items:
            return file_items
        return _get_event_archive_entries(db, limit=limit, since=since, until=until)

    def _rebuild_profiles_from_profile_archive(
        db: Any,
        *,
        limit: Optional[int] = None,
        after_db_id: Optional[int] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        facts = list_meiro_profile_facts(db, limit=limit)
        if facts and after_db_id is None and since is None and until is None:
            return facts
        profiles = rebuild_profiles_from_meiro_profile_batches(
            db,
            limit=limit,
            after_db_id=after_db_id,
            since=since,
            until=until,
        )
        if profiles:
            return profiles
        return rebuild_profiles_from_webhook_archive(limit=limit, since=since, until=until)

    def _record_auto_replay_run(
        db: Any,
        *,
        status: str,
        trigger: str,
        replay_mode: str,
        reason: Optional[str],
        started_at: str,
        completed_at: Optional[str],
        current_event_batch_db_id: Optional[int],
        event_archive_status: Dict[str, Any],
        archive_entries_used: Optional[int] = None,
        profiles_reconstructed: Optional[int] = None,
        quarantine_count: Optional[int] = None,
        persisted_count: Optional[int] = None,
        result_summary: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        history_entry = {
            "at": completed_at or started_at,
            "status": status,
            "trigger": trigger,
            "reason": reason,
            "archive_entries_seen": int(event_archive_status.get("entries") or 0),
            "archive_received_at": event_archive_status.get("last_received_at"),
            "event_batch_db_id_seen": current_event_batch_db_id,
        }
        if result_summary:
            history_entry["result_summary"] = result_summary
        append_auto_replay_history(history_entry)
        return record_meiro_replay_run(
            db,
            scope="auto",
            status=status,
            trigger=trigger,
            archive_source="events",
            replay_mode=replay_mode,
            reason=reason,
            started_at=started_at,
            completed_at=completed_at,
            latest_event_batch_db_id=current_event_batch_db_id,
            archive_entries_seen=int(event_archive_status.get("entries") or 0),
            archive_entries_used=archive_entries_used,
            profiles_reconstructed=profiles_reconstructed,
            quarantine_count=quarantine_count,
            persisted_count=persisted_count,
            result_json=result_summary or {},
        )

    def _get_auto_replay_history_items(db: Any, *, limit: int = 25) -> List[Dict[str, Any]]:
        items = list_meiro_replay_runs(db, scope="auto", limit=limit)
        if items:
            return items
        return get_auto_replay_history(limit=limit)

    def _flatten_event_archive_entries(entries: List[Dict[str, Any]]) -> List[Any]:
        return [event for entry in reversed(entries) for event in (entry.get("events") or [])]

    def _any_replace_batches(entries: List[Dict[str, Any]]) -> bool:
        return any(bool(entry.get("replace")) for entry in entries)

    def _rebuild_profiles_from_event_archive(
        db: Any,
        *,
        source_entries: Optional[List[Dict[str, Any]]] = None,
        only_profile_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
        after_db_id: Optional[int] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        profile_ids = [str(item).strip() for item in (only_profile_ids or []) if str(item).strip()]
        if profile_ids:
            fact_events = list_meiro_event_facts(db, profile_ids=profile_ids)
            if fact_events:
                return rebuild_profiles_from_meiro_events_fn(
                    fact_events,
                    dedup_config=get_pull_config(),
                    only_profile_ids=profile_ids,
                )
            state_profiles = list_meiro_event_profile_state(db, profile_ids=profile_ids)
            if state_profiles:
                return state_profiles
            source_entries = _get_event_archive_entries(db, limit=None, since=since, until=until)
        elif source_entries is None:
            source_entries = _get_event_archive_entries(
                db,
                limit=limit,
                after_db_id=after_db_id,
                since=since,
                until=until,
            )
        return rebuild_profiles_from_meiro_events_fn(
            _flatten_event_archive_entries(source_entries or []),
            dedup_config=get_pull_config(),
            only_profile_ids=profile_ids,
        )

    def _load_archived_profiles_for_replay(
        db: Any,
        *,
        replay_mode: str,
        archive_source: str,
        archive_limit: int,
        date_from: Optional[str],
        date_to: Optional[str],
        incremental_after_db_id: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool, Dict[str, Any]]:
        profile_archive_status = _get_profile_archive_status(db)
        event_archive_status = _get_event_archive_status(db)
        use_event_archive = _prefer_event_archive(
            archive_source,
            profile_archive_status,
            event_archive_status,
            primary_source=str(get_pull_config().get("primary_ingest_source") or "profiles"),
        )
        replay_scope: Dict[str, Any] = {"incremental": False, "replace_profile_ids": []}
        if use_event_archive and incremental_after_db_id is not None and replay_mode != "date_range":
            archive_entries = _get_event_archive_entries(db, limit=None, after_db_id=incremental_after_db_id)
            if _any_replace_batches(archive_entries):
                incremental_after_db_id = None
            else:
                replace_profile_ids = extract_customer_ids_from_meiro_events_fn(_flatten_event_archive_entries(archive_entries))
                archived_profiles = _rebuild_profiles_from_event_archive(
                    db,
                    only_profile_ids=replace_profile_ids,
                    since=date_from,
                    until=date_to,
                )
                replay_scope = {
                    "incremental": True,
                    "replace_profile_ids": replace_profile_ids,
                    "after_db_id": incremental_after_db_id,
                }
                return archive_entries, archived_profiles, True, replay_scope
        if replay_mode == "all":
            if use_event_archive:
                archive_entries = _get_event_archive_entries_for_replay(db, limit=None)
                archived_profiles = _rebuild_profiles_from_event_archive(db, source_entries=archive_entries)
            else:
                archive_entries = _get_profile_archive_entries(db, limit=None)
                archived_profiles = _rebuild_profiles_from_profile_archive(db, limit=None)
        elif replay_mode == "date_range":
            if use_event_archive:
                archive_entries = _get_event_archive_entries_for_replay(db, limit=None, since=date_from, until=date_to)
                archived_profiles = _rebuild_profiles_from_event_archive(
                    db,
                    source_entries=archive_entries,
                    since=date_from,
                    until=date_to,
                )
            else:
                archive_entries = _get_profile_archive_entries(db, limit=None, since=date_from, until=date_to)
                archived_profiles = _rebuild_profiles_from_profile_archive(
                    db,
                    limit=None,
                    since=date_from,
                    until=date_to,
                )
        else:
            if use_event_archive:
                archive_entries = _get_event_archive_entries_for_replay(db, limit=archive_limit)
                archived_profiles = _rebuild_profiles_from_event_archive(db, source_entries=archive_entries)
            else:
                archive_entries = _get_profile_archive_entries(db, limit=archive_limit)
                archived_profiles = _rebuild_profiles_from_profile_archive(db, limit=archive_limit)
        return archive_entries, archived_profiles, use_event_archive, replay_scope

    def _evaluate_auto_replay_guardrails(
        *,
        trigger: str,
        pull_config: Dict[str, Any],
        auto_replay_state: Dict[str, Any],
        event_archive_status: Dict[str, Any],
        mapping_state: Dict[str, Any],
    ) -> Optional[str]:
        mode = str(pull_config.get("auto_replay_mode") or "disabled")
        if mode == "disabled":
            return "Auto-replay is disabled."
        if str(pull_config.get("primary_ingest_source") or "profiles") != "events":
            return "Primary ingest source is not raw events."
        if not bool(event_archive_status.get("available")):
            return "Raw event archive is empty."
        if pull_config.get("auto_replay_require_mapping_approval", True):
            approval = str(((mapping_state.get("approval") or {}).get("status") or "unreviewed")).lower()
            if approval != "approved":
                return "Mapping approval is required before auto-replay."
        manual_override = trigger == "manual"
        current_batch_db_id_raw = event_archive_status.get("latest_batch_db_id")
        try:
            current_batch_db_id = int(current_batch_db_id_raw) if current_batch_db_id_raw is not None else None
        except Exception:
            current_batch_db_id = None
        last_seen_batch_db_id_raw = auto_replay_state.get("last_event_batch_db_id_seen")
        try:
            last_seen_batch_db_id = int(last_seen_batch_db_id_raw) if last_seen_batch_db_id_raw is not None else None
        except Exception:
            last_seen_batch_db_id = None
        current_entries = int(event_archive_status.get("entries") or 0)
        last_seen_entries = int(auto_replay_state.get("last_archive_entries_seen") or 0)
        no_new_db_batches = (
            current_batch_db_id is not None
            and last_seen_batch_db_id is not None
            and current_batch_db_id <= last_seen_batch_db_id
        )
        no_new_archive_entries = current_entries <= last_seen_entries
        if (no_new_db_batches or (current_batch_db_id is None and no_new_archive_entries)) and not manual_override:
            return "No new raw-event archive batches since the last auto-replay decision."
        if mode == "after_batch" and not manual_override and trigger != "after_batch":
            return "Auto-replay is configured to run after successful event batches."
        if mode == "interval" and not manual_override:
            interval_minutes = max(1, int(pull_config.get("auto_replay_interval_minutes") or 15))
            last_attempted = _parse_iso_datetime(auto_replay_state.get("last_attempted_at"))
            if last_attempted:
                next_allowed = last_attempted + timedelta(minutes=interval_minutes)
                if next_allowed > datetime.now(timezone.utc):
                    return f"Waiting for the {interval_minutes}-minute auto-replay interval."
        return None

    def _run_auto_replay(db: Any, *, trigger: str) -> Dict[str, Any]:
        pull_config = get_pull_config()
        mapping_state = get_mapping_state()
        event_archive_status = _get_event_archive_status(db)
        auto_replay_state = get_auto_replay_state()
        now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        guardrail_reason = _evaluate_auto_replay_guardrails(
            trigger=trigger,
            pull_config=pull_config,
            auto_replay_state=auto_replay_state,
            event_archive_status=event_archive_status,
            mapping_state=mapping_state,
        )
        current_event_batch_db_id_raw = event_archive_status.get("latest_batch_db_id")
        try:
            current_event_batch_db_id = int(current_event_batch_db_id_raw) if current_event_batch_db_id_raw is not None else None
        except Exception:
            current_event_batch_db_id = None
        last_seen_batch_db_id_raw = auto_replay_state.get("last_event_batch_db_id_seen")
        try:
            last_seen_batch_db_id = int(last_seen_batch_db_id_raw) if last_seen_batch_db_id_raw is not None else None
        except Exception:
            last_seen_batch_db_id = None
        if guardrail_reason:
            should_advance_checkpoint = guardrail_reason.startswith("No new raw-event archive batches")
            state = update_auto_replay_state(
                {
                    "last_attempted_at": now_iso,
                    "last_status": "skipped",
                    "last_reason": guardrail_reason,
                    "last_trigger": trigger,
                    "last_archive_entries_seen": int(event_archive_status.get("entries") or 0) if should_advance_checkpoint else int(auto_replay_state.get("last_archive_entries_seen") or 0),
                    "last_archive_received_at": event_archive_status.get("last_received_at") if should_advance_checkpoint else auto_replay_state.get("last_archive_received_at"),
                    "last_event_batch_db_id_seen": current_event_batch_db_id if should_advance_checkpoint else auto_replay_state.get("last_event_batch_db_id_seen"),
                }
            )
            _record_auto_replay_run(
                db,
                status="skipped",
                trigger=trigger,
                replay_mode=str(pull_config.get("replay_mode") or "last_n").strip().lower() or "last_n",
                reason=guardrail_reason,
                started_at=now_iso,
                completed_at=now_iso,
                current_event_batch_db_id=current_event_batch_db_id,
                event_archive_status=event_archive_status,
            )
            return {"ok": False, "status": "skipped", "reason": guardrail_reason, "state": state}

        replay_mode = str(pull_config.get("replay_mode") or "last_n").strip().lower()
        archive_limit = int(pull_config.get("replay_archive_limit") or 5000)
        date_from = pull_config.get("replay_date_from") if replay_mode == "date_range" else None
        date_to = pull_config.get("replay_date_to") if replay_mode == "date_range" else None
        incremental_after_db_id = (
            last_seen_batch_db_id
            if trigger != "manual" and last_seen_batch_db_id is not None
            else None
        )
        update_auto_replay_state(
            {
                "last_attempted_at": now_iso,
                "last_status": "running",
                "last_reason": None,
                "last_trigger": trigger,
            }
        )
        archive_entries: List[Dict[str, Any]] = []
        archived_profiles: List[Dict[str, Any]] = []
        replay_scope: Dict[str, Any] = {}
        try:
            archive_entries, archived_profiles, use_event_archive, replay_scope = _load_archived_profiles_for_replay(
                db,
                replay_mode=replay_mode,
                archive_source="events",
                archive_limit=archive_limit,
                date_from=date_from,
                date_to=date_to,
                incremental_after_db_id=incremental_after_db_id,
            )
            if not use_event_archive or not archive_entries or not archived_profiles:
                state = update_auto_replay_state(
                    {
                        "last_status": "skipped",
                        "last_reason": "No raw-event archive data was available for auto-replay.",
                        "last_archive_entries_seen": int(event_archive_status.get("entries") or 0),
                        "last_archive_received_at": event_archive_status.get("last_received_at"),
                        "last_event_batch_db_id_seen": current_event_batch_db_id,
                    }
                )
                _record_auto_replay_run(
                    db,
                    status="skipped",
                    trigger=trigger,
                    replay_mode=replay_mode,
                    reason=state.get("last_reason"),
                    started_at=now_iso,
                    completed_at=now_iso,
                    current_event_batch_db_id=current_event_batch_db_id,
                    event_archive_status=event_archive_status,
                )
                return {"ok": False, "status": "skipped", "reason": state.get("last_reason"), "state": state}

            mapping = _build_saved_mapping_config()
            settings = get_settings_obj()
            revenue_config = settings.revenue_config
            preflight = canonicalize_meiro_profiles_fn(
                archived_profiles,
                mapping=mapping,
                revenue_config=(revenue_config.model_dump() if hasattr(revenue_config, "model_dump") else revenue_config),
                dedup_config=pull_config,
            )
            import_summary = preflight.get("import_summary") or {}
            total_profiles = int(import_summary.get("total") or len(archived_profiles) or 0)
            quarantine_count = int(
                import_summary.get("quarantined")
                or import_summary.get("quarantine_count")
                or preflight.get("quarantine_count")
                or 0
            )
            quarantine_share = (quarantine_count / total_profiles) if total_profiles > 0 else 0.0
            quarantine_threshold_raw = pull_config.get("auto_replay_quarantine_spike_threshold_pct")
            try:
                quarantine_threshold_pct = int(quarantine_threshold_raw) if quarantine_threshold_raw is not None else 40
            except Exception:
                quarantine_threshold_pct = 40
            quarantine_threshold = max(0, min(100, quarantine_threshold_pct)) / 100.0
            if quarantine_share > quarantine_threshold:
                reason = (
                    f"Auto-replay blocked because quarantined journeys reached {round(quarantine_share * 100, 2)}% "
                    f"of the replay set, above the configured {round(quarantine_threshold * 100, 2)}% threshold."
                )
                state = update_auto_replay_state(
                    {
                        "last_completed_at": now_iso,
                        "last_status": "blocked",
                        "last_reason": reason,
                        "last_archive_entries_seen": int(event_archive_status.get("entries") or 0),
                        "last_archive_received_at": event_archive_status.get("last_received_at"),
                        "last_event_batch_db_id_seen": current_event_batch_db_id,
                        "last_result_summary": {
                            "archive_entries_used": len(archive_entries),
                            "profiles_reconstructed": len(archived_profiles),
                            "quarantine_count": quarantine_count,
                            "quarantine_share_pct": round(quarantine_share * 100, 2),
                            "latest_event_batch_db_id": current_event_batch_db_id,
                            "incremental": bool(replay_scope.get("incremental")),
                            "replace_profile_count": len(replay_scope.get("replace_profile_ids") or []),
                        },
                    }
                )
                _record_auto_replay_run(
                    db,
                    status="blocked",
                    trigger=trigger,
                    replay_mode=replay_mode,
                    reason=reason,
                    started_at=now_iso,
                    completed_at=now_iso,
                    current_event_batch_db_id=current_event_batch_db_id,
                    event_archive_status=event_archive_status,
                    archive_entries_used=len(archive_entries),
                    profiles_reconstructed=len(archived_profiles),
                    quarantine_count=quarantine_count,
                    result_summary=state.get("last_result_summary") or {},
                )
                return {"ok": False, "status": "blocked", "reason": reason, "state": state}

            replay_context_path = get_data_dir_obj() / "meiro_replay_context.json"
            replay_diagnostics = _build_event_replay_reconstruction_diagnostics(
                archive_entries=archive_entries,
                archived_profiles=archived_profiles,
            )
            replay_snapshot = create_meiro_replay_snapshot(
                db,
                source_kind="events",
                profiles_json=archived_profiles,
                replay_mode=replay_mode,
                latest_event_batch_db_id=current_event_batch_db_id,
                archive_entries_used=len(archive_entries),
                context_json={
                    "archive_source": "events",
                    "replay_mode": replay_mode,
                    "archive_entries_used": len(archive_entries),
                    "latest_event_batch_db_id": current_event_batch_db_id,
                    "event_reconstruction_diagnostics": replay_diagnostics,
                    "replace_profile_ids": replay_scope.get("replace_profile_ids") or [],
                    "incremental": bool(replay_scope.get("incremental")),
                    "auto_replay": True,
                    "auto_replay_trigger": trigger,
                },
            )
            replay_context_path.write_text(
                json.dumps(
                    {
                        "archive_source": "events",
                        "replay_mode": replay_mode,
                        "archive_entries_used": len(archive_entries),
                        "replay_snapshot_id": replay_snapshot.get("snapshot_id"),
                        "latest_event_batch_db_id": current_event_batch_db_id,
                        "event_reconstruction_diagnostics": replay_diagnostics,
                        "replace_profile_ids": replay_scope.get("replace_profile_ids") or [],
                        "incremental": bool(replay_scope.get("incremental")),
                        "auto_replay": True,
                        "auto_replay_trigger": trigger,
                    },
                    indent=2,
                )
            )
            import_result = import_journeys_from_cdp_fn(
                req=from_cdp_request_factory(
                    import_note=f"Auto-replayed from raw-event archive ({trigger})",
                    replay_snapshot_id=replay_snapshot.get("snapshot_id"),
                ),
                db=db,
            )
            completed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            state = update_auto_replay_state(
                {
                    "last_completed_at": completed_at,
                    "last_status": "success",
                    "last_reason": None,
                    "last_archive_entries_seen": int(event_archive_status.get("entries") or 0),
                    "last_archive_received_at": event_archive_status.get("last_received_at"),
                    "last_event_batch_db_id_seen": current_event_batch_db_id,
                    "last_result_summary": {
                        "archive_entries_used": len(archive_entries),
                        "profiles_reconstructed": len(archived_profiles),
                        "persisted_count": int(import_result.get("count") or 0),
                        "quarantine_count": int(import_result.get("quarantine_count") or 0),
                        "latest_event_batch_db_id": current_event_batch_db_id,
                        "incremental": bool(replay_scope.get("incremental")),
                        "replace_profile_count": len(replay_scope.get("replace_profile_ids") or []),
                    },
                }
            )
            _record_auto_replay_run(
                db,
                status="success",
                trigger=trigger,
                replay_mode=replay_mode,
                reason=None,
                started_at=now_iso,
                completed_at=completed_at,
                current_event_batch_db_id=current_event_batch_db_id,
                event_archive_status=event_archive_status,
                archive_entries_used=len(archive_entries),
                profiles_reconstructed=len(archived_profiles),
                quarantine_count=int(import_result.get("quarantine_count") or 0),
                persisted_count=int(import_result.get("count") or 0),
                result_summary=state.get("last_result_summary") or {},
            )
            return {
                "ok": True,
                "status": "success",
                "state": state,
                "import_result": import_result,
                "archive_entries_used": len(archive_entries),
                "profiles_reconstructed": len(archived_profiles),
                "incremental": bool(replay_scope.get("incremental")),
                "replace_profile_count": len(replay_scope.get("replace_profile_ids") or []),
            }
        except OperationalError as exc:
            if not _is_retryable_sqlite_operational_error(exc):
                raise
            logger.warning("Auto-replay deferred because SQLite was temporarily unavailable", exc_info=True)
            reason = (
                "Auto-replay could not access the SQLite store. "
                "The raw-event batch was saved and replay can be retried."
            )
            result_summary = {
                "archive_entries_used": len(archive_entries),
                "profiles_reconstructed": len(archived_profiles),
                "latest_event_batch_db_id": current_event_batch_db_id,
                "incremental": bool(replay_scope.get("incremental")),
                "replace_profile_count": len(replay_scope.get("replace_profile_ids") or []),
                "retryable": True,
                "error_class": exc.__class__.__name__,
            }
            state = update_auto_replay_state(
                {
                    "last_completed_at": now_iso,
                    "last_status": "unavailable",
                    "last_reason": reason,
                    "last_result_summary": result_summary,
                }
            )
            _record_auto_replay_run(
                db,
                status="unavailable",
                trigger=trigger,
                replay_mode=replay_mode,
                reason=reason,
                started_at=now_iso,
                completed_at=now_iso,
                current_event_batch_db_id=current_event_batch_db_id,
                event_archive_status=event_archive_status,
                archive_entries_used=len(archive_entries) or None,
                profiles_reconstructed=len(archived_profiles) or None,
                result_summary=result_summary,
            )
            return {
                "ok": False,
                "status": "unavailable",
                "reason": reason,
                "state": state,
                "retryable": True,
            }

    @router.get("/v1/meiro/api/status")
    def meiro_api_status():
        return services_meiro_api.get_safe_status()

    @router.post("/v1/meiro/api/check-login")
    def meiro_api_check_login():
        try:
            return services_meiro_api.check_login()
        except MeiroApiError as exc:
            _handle_meiro_api_error(exc)

    @router.get("/v1/meiro/audience/profile")
    def meiro_api_audience_profile(
        attribute: str = Query(..., min_length=1),
        value: str = Query(..., min_length=1),
        categoryId: Optional[str] = Query(None),
    ):
        try:
            return services_meiro_api.lookup_wbs_profile(attribute=attribute, value=value, category_id=categoryId)
        except MeiroApiError as exc:
            _handle_meiro_api_error(exc)

    @router.get("/v1/meiro/audience/segments")
    def meiro_api_audience_segments(
        attribute: str = Query(..., min_length=1),
        value: str = Query(..., min_length=1),
        tag: Optional[str] = Query(None),
    ):
        _ = tag
        try:
            return services_meiro_api.lookup_wbs_segments(attribute=attribute, value=value)
        except MeiroApiError as exc:
            _handle_meiro_api_error(exc)

    @router.get("/v1/meiro/native-campaigns")
    def meiro_api_native_campaigns(
        channel: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
        q: Optional[str] = Query(None),
        includeDeleted: bool = Query(False),
    ):
        try:
            return services_meiro_api.list_native_campaigns(
                channel=channel,
                limit=limit,
                offset=offset,
                q=q,
                include_deleted=includeDeleted,
            )
        except MeiroApiError as exc:
            _handle_meiro_api_error(exc)

    @router.get("/v1/meiro/native-campaigns/{channel}/{campaign_id}")
    def meiro_api_native_campaign_detail(channel: str, campaign_id: str):
        try:
            return services_meiro_api.get_native_campaign(channel=channel, campaign_id=campaign_id)
        except MeiroApiError as exc:
            _handle_meiro_api_error(exc)

    @router.get("/v1/meiro/reports/search")
    def meiro_reporting_search(
        q: str = Query(..., min_length=1),
        model: Optional[str] = Query(None),
        limit: int = Query(20, ge=1, le=100),
    ):
        try:
            return services_meiro_api.search_reporting_assets(q=q, model=model, limit=limit)
        except MeiroApiError as exc:
            _handle_meiro_api_error(exc)

    @router.get("/v1/meiro/reports/dashboard/{dashboard_id}")
    def meiro_reporting_dashboard(dashboard_id: int):
        try:
            return services_meiro_api.get_reporting_dashboard(dashboard_id)
        except MeiroApiError as exc:
            _handle_meiro_api_error(exc)

    @router.get("/v1/meiro/reports/card/{card_id}")
    def meiro_reporting_card(card_id: int):
        try:
            return services_meiro_api.get_reporting_card(card_id)
        except MeiroApiError as exc:
            _handle_meiro_api_error(exc)

    @router.post("/v1/meiro/reports/card/{card_id}/query-json")
    def meiro_reporting_card_query_json(
        card_id: int,
        parameters: Optional[List[Dict[str, Any]]] = Body(default=None),
        limit: int = Query(100, ge=1, le=1000),
    ):
        try:
            return services_meiro_api.query_reporting_card_json(card_id, parameters=parameters, limit=limit)
        except MeiroApiError as exc:
            _handle_meiro_api_error(exc)

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
                try:
                    meiro_cdp.save_config(api_base_url, api_key)
                except ValueError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
            meiro_cdp.update_last_test_at()
            return {"ok": True, "message": "Connection successful"}
        raise HTTPException(status_code=400, detail=result.get("message", "Connection failed"))

    @router.post("/api/connectors/meiro/connect")
    def meiro_connect(req: MeiroCDPConnectRequest):
        result = meiro_cdp.test_connection(req.api_base_url, req.api_key)
        if result.get("ok"):
            try:
                meiro_cdp.save_config(req.api_base_url, req.api_key)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            meiro_cdp.update_last_test_at()
            return {"message": "Connected to Meiro CDP", "connected": True}
        raise HTTPException(status_code=400, detail=result.get("message", "Connection failed"))

    @router.post("/api/connectors/meiro/save")
    def meiro_save(req: MeiroCDPConnectRequest):
        try:
            meiro_cdp.save_config(req.api_base_url, req.api_key)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
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
    def meiro_config(db=Depends(get_db_dependency)):
        meta = meiro_cdp.get_connection_metadata()
        webhook_url = f"{get_base_url_fn()}/api/connectors/meiro/profiles"
        event_webhook_url = f"{get_base_url_fn()}/api/connectors/meiro/events"
        event_archive_status = _get_event_archive_status(db)
        pull_config = get_pull_config()
        return {
            "connected": meiro_cdp.is_connected(),
            "api_base_url": meta["api_base_url"] if meta else None,
            "target_instance_url": get_target_instance_url(),
            "target_instance_host": get_target_instance_host(),
            "strict_instance_scope": True,
            "cdp_instance_scope": (meta.get("instance_scope") if meta else instance_scope(None)),
            "last_test_at": meta["last_test_at"] if meta else get_last_test_at(),
            "has_key": meta["has_key"] if meta else False,
            "webhook_url": webhook_url,
            "event_webhook_url": event_webhook_url,
            "webhook_last_received_at": get_webhook_last_received_at(),
            "webhook_received_count": get_webhook_received_count(),
            "event_webhook_last_received_at": event_archive_status.get("last_received_at"),
            "event_webhook_received_count": event_archive_status.get("events_received", 0),
            "webhook_has_secret": bool(get_webhook_secret() or os.getenv("MEIRO_WEBHOOK_SECRET", "").strip()),
            "primary_ingest_source": pull_config.get("primary_ingest_source", "profiles"),
            "auto_replay_state": get_auto_replay_state(),
        }

    @router.get("/api/connectors/meiro/readiness")
    def meiro_readiness(db=Depends(get_db_dependency)):
        config = meiro_config(db=db)
        mapping_state = get_mapping_state()
        profile_archive_status = _get_profile_archive_status(db)
        event_archive_status = _get_event_archive_status(db)
        pull_config = get_pull_config()
        raw_event_diagnostics = _build_raw_event_stream_diagnostics(
            event_archive_entries=_get_event_archive_entries(db, limit=100),
            webhook_events=get_webhook_events(limit=250),
        )
        return build_meiro_readiness(
            meiro_connected=meiro_cdp.is_connected(),
            meiro_config=config,
            mapping_state=mapping_state,
            archive_status=profile_archive_status,
            event_archive_status=event_archive_status,
            pull_config=pull_config,
            raw_event_diagnostics=raw_event_diagnostics,
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
        try:
            return meiro_cdp.list_events()
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.get("/api/connectors/meiro/segments")
    def meiro_segments():
        if not meiro_cdp.is_connected():
            raise HTTPException(status_code=401, detail="Meiro CDP not connected")
        try:
            return meiro_cdp.list_segments()
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

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
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
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

    @router.get("/api/connectors/meiro/profiles")
    @router.head("/api/connectors/meiro/profiles")
    def meiro_profiles_webhook_health():
        return {
            "ok": True,
            "service": "meiro_profiles_webhook",
            "message": "Webhook endpoint is reachable.",
            "received_count": get_webhook_received_count(),
            "last_received_at": get_webhook_last_received_at(),
        }

    @router.get("/api/connectors/meiro/profiles/health")
    @router.head("/api/connectors/meiro/profiles/health")
    def meiro_profiles_webhook_explicit_health():
        return {
            "ok": True,
            "service": "meiro_profiles_webhook",
            "message": "Explicit webhook health check.",
            "received_count": get_webhook_received_count(),
            "last_received_at": get_webhook_last_received_at(),
        }

    @router.get("/api/connectors/meiro/events/health")
    @router.head("/api/connectors/meiro/events/health")
    def meiro_events_webhook_explicit_health(db=Depends(get_db_dependency)):
        event_archive_status = _get_event_archive_status(db)
        return {
            "ok": True,
            "service": "meiro_events_webhook",
            "message": "Explicit raw event webhook health check.",
            "received_count": event_archive_status.get("events_received", 0),
            "last_received_at": event_archive_status.get("last_received_at"),
        }

    @router.post("/api/connectors/meiro/profiles")
    async def meiro_receive_profiles(
        request: Request,
        x_meiro_webhook_secret: Optional[str] = Header(None, alias="X-Meiro-Webhook-Secret"),
        db=Depends(get_db_dependency),
    ):
        try:
            try:
                webhook_secret = get_webhook_secret() or os.getenv("MEIRO_WEBHOOK_SECRET", "").strip()
                if webhook_secret and (not x_meiro_webhook_secret or x_meiro_webhook_secret != webhook_secret):
                    _record_webhook_diagnostic_event(
                        request=request,
                        parser_version=meiro_parser_version,
                        ingest_kind="profiles",
                        payload_shape="unknown",
                        status_code=401,
                        outcome="error",
                        error_class="auth",
                        error_detail="Invalid or missing X-Meiro-Webhook-Secret",
                    )
                    raise HTTPException(status_code=401, detail="Invalid or missing X-Meiro-Webhook-Secret")

                body = await request.json()
            except HTTPException:
                raise
            except Exception as exc:
                _record_webhook_diagnostic_event(
                    request=request,
                    parser_version=meiro_parser_version,
                    ingest_kind="profiles",
                    payload_shape="invalid_json",
                    status_code=400,
                    outcome="error",
                    error_class="invalid_json",
                    error_detail=str(exc),
                )
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
                _record_webhook_diagnostic_event(
                    request=request,
                    parser_version=meiro_parser_version,
                    ingest_kind="profiles",
                    payload_shape="unknown",
                    status_code=400,
                    outcome="error",
                    error_class="invalid_payload_shape",
                    error_detail="Body must be JSON array or object with 'profiles' key",
                )
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
                    **archive_source_metadata(),
                    "profiles": profiles,
                }
            )
            raw_batch = None
            raw_batch_status = {"ok": True, "stored": True, "warning": None}
            try:
                raw_batch = record_meiro_raw_batch_fn(
                    db,
                    source_kind="profiles",
                    ingestion_channel="webhook",
                    payload_json={
                        "received_at": now_iso,
                        "parser_version": meiro_parser_version,
                        "replace": bool(replace),
                        "payload_shape": "array" if isinstance(body, list) else "object",
                        "received_count": int(len(profiles)),
                        **archive_source_metadata(),
                        "profiles": profiles,
                    },
                    received_at=now_iso,
                    parser_version=meiro_parser_version,
                    payload_shape="array" if isinstance(body, list) else "object",
                    replace=bool(replace),
                    records_count=int(len(profiles)),
                    metadata_json={
                        "ip": request.client.host if request.client else None,
                        "user_agent": (request.headers.get("user-agent") or "")[:256] or None,
                    },
                )
            except MeiroRawBatchUnavailableError as exc:
                logger.warning("Meiro raw-batch storage is unavailable; continuing with file archive + profile facts", exc_info=True)
                raw_batch_status = {
                    "ok": False,
                    "stored": False,
                    "warning": "DB raw-batch storage is unavailable. File archive and profile facts were still updated.",
                    "reason": str(exc),
                }
            upsert_meiro_profile_facts(
                db,
                profiles=profiles,
                raw_batch_db_id=(int(raw_batch.id) if getattr(raw_batch, "id", None) is not None else None),
                reset=bool(replace),
            )
            append_webhook_event(
                {
                    "received_at": now_iso,
                    "received_count": int(len(profiles)),
                    "stored_total": int(len(to_store)),
                    "replace": bool(replace),
                    "parser_version": meiro_parser_version,
                    "ingest_kind": "profiles",
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
                    "status_code": 200,
                    "outcome": "success",
                    "error_class": None,
                    "warning_class": None if raw_batch_status["ok"] else "raw_batch_unavailable",
                    "warning_detail": raw_batch_status["warning"],
                },
                max_items=100,
            )
            return JSONResponse(
                status_code=200,
                content={
                    "ok": True,
                    "received": len(profiles),
                    "stored_total": len(to_store),
                    "raw_batch": raw_batch_status,
                    "message": "Profiles saved. Use Import from CDP in Data Sources to load into attribution.",
                },
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Unhandled Meiro webhook failure")
            _record_webhook_diagnostic_event(
                request=request,
                parser_version=meiro_parser_version,
                ingest_kind="profiles",
                payload_shape="unknown",
                status_code=503,
                outcome="error",
                error_class="app_error",
                error_detail=str(exc),
            )
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "error_code": "webhook_upstream_unavailable",
                    "detail": "Webhook processing failed before a complete response could be returned.",
                    "reason": str(exc),
                },
            )

    @router.post("/api/connectors/meiro/events")
    async def meiro_receive_events(
        request: Request,
        x_meiro_webhook_secret: Optional[str] = Header(None, alias="X-Meiro-Webhook-Secret"),
        db=Depends(get_db_dependency),
    ):
        try:
            try:
                webhook_secret = get_webhook_secret() or os.getenv("MEIRO_WEBHOOK_SECRET", "").strip()
                if webhook_secret and (not x_meiro_webhook_secret or x_meiro_webhook_secret != webhook_secret):
                    _record_webhook_diagnostic_event(
                        request=request,
                        parser_version=meiro_parser_version,
                        ingest_kind="events",
                        payload_shape="unknown",
                        status_code=401,
                        outcome="error",
                        error_class="auth",
                        error_detail="Invalid or missing X-Meiro-Webhook-Secret",
                    )
                    raise HTTPException(status_code=401, detail="Invalid or missing X-Meiro-Webhook-Secret")
                body = await request.json()
            except HTTPException:
                raise
            except Exception as exc:
                _record_webhook_diagnostic_event(
                    request=request,
                    parser_version=meiro_parser_version,
                    ingest_kind="events",
                    payload_shape="invalid_json",
                    status_code=400,
                    outcome="error",
                    error_class="invalid_json",
                    error_detail=str(exc),
                )
                raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")

            if isinstance(body, list):
                events = body
                replace = False
            elif isinstance(body, dict):
                events = body.get("events")
                if not isinstance(events, list):
                    data_payload = body.get("data")
                    if isinstance(data_payload, list):
                        events = data_payload
                if not isinstance(events, list) and isinstance(body.get("event_payload"), dict):
                    events = [body]
                if not isinstance(events, list):
                    events = [body]
                replace = bool(body.get("replace", False))
            else:
                _record_webhook_diagnostic_event(
                    request=request,
                    parser_version=meiro_parser_version,
                    ingest_kind="events",
                    payload_shape="unknown",
                    status_code=400,
                    outcome="error",
                    error_class="invalid_payload_shape",
                    error_detail="Body must be JSON array or object with 'events' key",
                )
                raise HTTPException(status_code=400, detail="Body must be JSON array or object with 'events' key")

            out_path = get_data_dir_obj() / "meiro_cdp_events.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            snapshot_limit = 1000
            if replace or not out_path.exists():
                to_store = list(events)[-snapshot_limit:]
            else:
                existing_tail: list[Any] = []
                try:
                    if out_path.stat().st_size <= 5_000_000:
                        existing = json.loads(out_path.read_text())
                        existing_tail = (existing if isinstance(existing, list) else [])[-snapshot_limit:]
                except Exception:
                    existing_tail = []
                to_store = (existing_tail + list(events))[-snapshot_limit:]
            out_path.write_text(json.dumps(to_store, separators=(",", ":")))

            now_iso = datetime.utcnow().isoformat() + "Z"
            set_webhook_received(count_delta=len(events), last_received_at=now_iso)
            payload_excerpt, payload_truncated, payload_bytes = _safe_json_excerpt(body)
            event_names_detected, channels_detected = _extract_event_payload_hints(events)
            append_event_archive_entry(
                {
                    "received_at": now_iso,
                    "parser_version": meiro_parser_version,
                    "replace": bool(replace),
                    "payload_shape": "array" if isinstance(body, list) else "object",
                    "received_count": int(len(events)),
                    **archive_source_metadata(),
                    "events": events,
                }
            )
            raw_batch = None
            raw_batch_status = {"ok": True, "stored": True, "warning": None}
            try:
                raw_batch = record_meiro_raw_batch_fn(
                    db,
                    source_kind="events",
                    ingestion_channel="webhook",
                    payload_json={
                        "received_at": now_iso,
                        "parser_version": meiro_parser_version,
                        "replace": bool(replace),
                        "payload_shape": "array" if isinstance(body, list) else "object",
                        "received_count": int(len(events)),
                        **archive_source_metadata(),
                        "events": events,
                    },
                    received_at=now_iso,
                    parser_version=meiro_parser_version,
                    payload_shape="array" if isinstance(body, list) else "object",
                    replace=bool(replace),
                    records_count=int(len(events)),
                    metadata_json={
                        "ip": request.client.host if request.client else None,
                        "user_agent": (request.headers.get("user-agent") or "")[:256] or None,
                    },
                )
            except MeiroRawBatchUnavailableError as exc:
                logger.warning("Meiro raw-batch storage is unavailable; continuing with file archive + downstream persistence", exc_info=True)
                raw_batch_status = {
                    "ok": False,
                    "stored": False,
                    "warning": "DB raw-batch storage is unavailable. File archive and event-derived persistence were still updated.",
                    "reason": str(exc),
                }
            event_facts_status = {"ok": True, "stored": True, "warning": None}
            try:
                upsert_meiro_event_facts(
                    db,
                    raw_events=events,
                    raw_batch_db_id=(int(raw_batch.id) if getattr(raw_batch, "id", None) is not None else None),
                    reset=bool(replace),
                )
            except MeiroEventFactsUnavailableError as exc:
                logger.warning("Canonical Meiro event facts are unavailable; continuing with archive + profile-state persistence", exc_info=True)
                event_facts_status = {
                    "ok": False,
                    "stored": False,
                    "warning": "Canonical event-facts storage is unavailable. Raw archive and event-derived profile state were still updated.",
                    "reason": str(exc),
                }
            rebuilt_profiles = rebuild_profiles_from_meiro_events_fn(events, dedup_config=get_pull_config())
            event_profile_state_status = {"ok": True, "stored": True, "warning": None}
            try:
                upsert_meiro_event_profile_state(
                    db,
                    profiles=rebuilt_profiles,
                    latest_event_batch_db_id=(int(raw_batch.id) if getattr(raw_batch, "id", None) is not None else None),
                    reset=bool(replace),
                )
            except MeiroEventProfileStateUnavailableError as exc:
                logger.warning("Canonical Meiro event-derived profile state is unavailable; continuing with archive + event facts", exc_info=True)
                event_profile_state_status = {
                    "ok": False,
                    "stored": False,
                    "warning": "Canonical event-derived profile-state storage is unavailable. Raw archive and event facts were still updated.",
                    "reason": str(exc),
                }
            payload_analysis = _analyze_payload(rebuilt_profiles)
            warning_class = None
            warning_detail = None
            if not raw_batch_status["ok"]:
                warning_class = "raw_batch_unavailable"
                warning_detail = raw_batch_status["warning"]
            elif not event_facts_status["ok"]:
                warning_class = "event_facts_unavailable"
                warning_detail = event_facts_status["warning"]
            elif not event_profile_state_status["ok"]:
                warning_class = "event_profile_state_unavailable"
                warning_detail = event_profile_state_status["warning"]
            append_webhook_event(
                {
                    "received_at": now_iso,
                    "received_count": int(len(events)),
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
                    "conversion_event_names": event_names_detected,
                    "channels_detected": channels_detected,
                    "payload_analysis": payload_analysis,
                    "status_code": 200,
                    "outcome": "success",
                    "error_class": None,
                    "ingest_kind": "events",
                    "reconstructed_profiles": len(rebuilt_profiles),
                    "warning_class": warning_class,
                    "warning_detail": warning_detail,
                },
                max_items=100,
            )
            auto_replay_result = None
            auto_replay_mode = str(get_pull_config().get("auto_replay_mode") or "disabled")
            if auto_replay_mode == "after_batch":
                try:
                    auto_replay_result = _run_auto_replay(db, trigger="after_batch")
                except Exception as exc:
                    logger.exception("Auto-replay failed after raw event batch")
                    auto_replay_result = {"ok": False, "status": "error", "reason": str(exc)}
            return JSONResponse(
                status_code=200,
                content={
                    "ok": True,
                    "received": len(events),
                    "stored_total": len(to_store),
                    "reconstructed_profiles": len(rebuilt_profiles),
                    "message": "Events saved. Use Replay archived webhook payloads or import from the event archive to build journeys.",
                    "raw_batch": raw_batch_status,
                    "event_facts": event_facts_status,
                    "event_profile_state": event_profile_state_status,
                    "auto_replay": auto_replay_result,
                },
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Unhandled Meiro event webhook failure")
            _record_webhook_diagnostic_event(
                request=request,
                parser_version=meiro_parser_version,
                ingest_kind="events",
                payload_shape="unknown",
                status_code=503,
                outcome="error",
                error_class="app_error",
                error_detail=str(exc),
            )
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "error_code": "webhook_upstream_unavailable",
                    "detail": "Event webhook processing failed before a complete response could be returned.",
                    "reason": str(exc),
                },
            )

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

    @router.get("/api/connectors/meiro/webhook/diagnostics")
    def meiro_webhook_diagnostics(limit: int = Query(100, ge=10, le=500)):
        events = get_webhook_events(limit=limit)
        success_count = 0
        error_count = 0
        error_classes: Dict[str, int] = {}
        latest_success = None
        latest_error = None
        for event in events:
            outcome = str(event.get("outcome") or ("success" if int(event.get("status_code") or 0) < 400 else "error"))
            if outcome == "success":
                success_count += 1
                latest_success = latest_success or event
            else:
                error_count += 1
                error_class = str(event.get("error_class") or "unknown")
                error_classes[error_class] = error_classes.get(error_class, 0) + 1
                latest_error = latest_error or event
        notes: List[str] = []
        if error_count == 0:
            notes.append("No server-side webhook errors are recorded in recent events.")
            notes.append("If Pipes shows an ngrok HTML 503 page, the failure is happening before the request reaches this app.")
        else:
            notes.append("Server-side errors shown here only cover requests that reached the webhook route.")
        return {
            "ok": True,
            "health_url": f"{get_base_url_fn()}/api/connectors/meiro/profiles/health",
            "received_count": get_webhook_received_count(),
            "last_received_at": get_webhook_last_received_at(),
            "recent_success_count": success_count,
            "recent_error_count": error_count,
            "recent_error_classes": error_classes,
            "latest_success": latest_success,
            "latest_error": latest_error,
            "notes": notes,
        }

    @router.get("/api/connectors/meiro/webhook/archive-status")
    def meiro_webhook_archive_status(db=Depends(get_db_dependency)):
        return _get_profile_archive_status(db)

    @router.get("/api/connectors/meiro/events/archive-status")
    def meiro_event_archive_status(db=Depends(get_db_dependency)):
        return _get_event_archive_status(db)

    @router.get("/api/connectors/meiro/webhook/archive")
    def meiro_webhook_archive(limit: int = Query(25, ge=1, le=500), db=Depends(get_db_dependency)):
        items = _get_profile_archive_entries(db, limit=limit)
        return {"items": items, "total": len(items)}

    @router.get("/api/connectors/meiro/events/archive")
    def meiro_event_archive(limit: int = Query(25, ge=1, le=500), db=Depends(get_db_dependency)):
        items = query_event_archive_entries(limit=limit)
        if not items:
            items = _get_event_archive_entries(db, limit=limit)
        return {"items": items, "total": len(items)}

    @router.get("/api/connectors/meiro/events/contract-readiness")
    def meiro_event_contract_readiness(limit: int = Query(200, ge=1, le=5000)):
        return build_event_contract_readiness(query_event_archive_entries(limit=limit))

    @router.post("/api/connectors/meiro/events/contract-sample")
    def meiro_event_contract_sample(db=Depends(get_db_dependency)):
        now_iso = datetime.utcnow().isoformat() + "Z"
        events = build_sample_contract_events()
        append_event_archive_entry(
            {
                "received_at": now_iso,
                "parser_version": meiro_parser_version,
                "replace": False,
                "payload_shape": "contract_sample",
                "received_count": len(events),
                **archive_source_metadata(),
                "events": events,
            }
        )
        set_webhook_received(count_delta=len(events), last_received_at=now_iso)
        raw_batch_status = {"ok": True, "stored": True, "warning": None}
        raw_batch = None
        try:
            raw_batch = record_meiro_raw_batch_fn(
                db,
                source_kind="events",
                ingestion_channel="contract_sample",
                payload_json={
                    "received_at": now_iso,
                    "parser_version": meiro_parser_version,
                    "replace": False,
                    "payload_shape": "contract_sample",
                    "received_count": len(events),
                    "events": events,
                },
                received_at=now_iso,
                parser_version=meiro_parser_version,
                payload_shape="contract_sample",
                replace=False,
                records_count=len(events),
                metadata_json={"source": "contract_sample"},
            )
        except MeiroRawBatchUnavailableError as exc:
            raw_batch_status = {
                "ok": False,
                "stored": False,
                "warning": "DB raw-batch storage is unavailable. File archive was still updated.",
                "reason": str(exc),
            }
        try:
            upsert_meiro_event_facts(
                db,
                raw_events=events,
                raw_batch_db_id=(int(raw_batch.id) if getattr(raw_batch, "id", None) is not None else None),
                reset=False,
            )
        except MeiroEventFactsUnavailableError:
            pass
        rebuilt_profiles = rebuild_profiles_from_meiro_events_fn(events, dedup_config=get_pull_config())
        try:
            upsert_meiro_event_profile_state(
                db,
                profiles=rebuilt_profiles,
                latest_event_batch_db_id=(int(raw_batch.id) if getattr(raw_batch, "id", None) is not None else None),
                reset=False,
            )
        except MeiroEventProfileStateUnavailableError:
            pass
        readiness = build_event_contract_readiness(query_event_archive_entries(limit=200))
        return {
            "ok": True,
            "received": len(events),
            "reconstructed_profiles": len(rebuilt_profiles),
            "raw_batch": raw_batch_status,
            "readiness": readiness,
        }

    @router.get("/api/connectors/meiro/quarantine")
    def meiro_quarantine_runs(
        limit: int = Query(10, ge=1, le=100),
        source: Optional[str] = Query(None),
    ):
        items = get_quarantine_runs(limit=limit, source=source)
        return {"items": items, "total": len(items)}

    @router.get("/api/connectors/meiro/quarantine/{run_id}")
    def meiro_quarantine_run_detail(run_id: str):
        item = get_quarantine_run(run_id)
        if not item:
            raise HTTPException(status_code=404, detail="Quarantine run not found")
        return item

    @router.post("/api/connectors/meiro/webhook/reprocess")
    def meiro_webhook_reprocess(
        payload: MeiroWebhookReprocessRequest = Body(default=MeiroWebhookReprocessRequest()),
        db=Depends(get_db_dependency),
    ):
        replay_mode = str(payload.replay_mode or "last_n").strip().lower()
        if replay_mode not in {"all", "last_n", "date_range"}:
            replay_mode = "last_n"
        archive_source = str(payload.archive_source or "auto").strip().lower()
        if archive_source not in {"auto", "profiles", "events"}:
            archive_source = "auto"
        archive_limit = payload.archive_limit or 5000
        date_from = payload.date_from if replay_mode == "date_range" else None
        date_to = payload.date_to if replay_mode == "date_range" else None

        profile_archive_status = _get_profile_archive_status(db)
        event_archive_status = _get_event_archive_status(db)
        use_event_archive = _prefer_event_archive(
            archive_source,
            profile_archive_status,
            event_archive_status,
            primary_source=str(get_pull_config().get("primary_ingest_source") or "profiles"),
        )

        archive_entries, archived_profiles, use_event_archive, replay_scope = _load_archived_profiles_for_replay(
            db,
            replay_mode=replay_mode,
            archive_source=archive_source,
            archive_limit=archive_limit,
            date_from=date_from,
            date_to=date_to,
        )
        if not archived_profiles:
            raise HTTPException(status_code=404, detail="No archived webhook payloads found to reprocess.")
        mapping_state = get_mapping_state()

        replay_context_path = get_data_dir_obj() / "meiro_replay_context.json"

        result: Dict[str, Any] = {
            "reprocessed_profiles": len(archived_profiles),
            "archive_entries_used": len(archive_entries),
            "archive_source": "events" if use_event_archive else "profiles",
            "replay_mode": replay_mode,
            "date_from": date_from,
            "date_to": date_to,
            "parser_version": meiro_parser_version,
            "mapping_version": mapping_state.get("version") or 0,
            "mapping_approval_status": ((mapping_state.get("approval") or {}).get("status") or "unreviewed"),
            "persisted_to_attribution": False,
        }
        if use_event_archive:
            result["event_reconstruction_diagnostics"] = _build_event_replay_reconstruction_diagnostics(
                archive_entries=archive_entries,
                archived_profiles=archived_profiles,
            )
        if payload.persist_to_attribution:
            replay_snapshot = create_meiro_replay_snapshot(
                db,
                source_kind="events" if use_event_archive else "profiles",
                profiles_json=archived_profiles,
                replay_mode=replay_mode,
                latest_event_batch_db_id=(event_archive_status.get("latest_batch_db_id") if use_event_archive else None),
                archive_entries_used=len(archive_entries),
                context_json={
                    "archive_source": "events" if use_event_archive else "profiles",
                    "replay_mode": replay_mode,
                    "archive_entries_used": len(archive_entries),
                    "event_reconstruction_diagnostics": result.get("event_reconstruction_diagnostics") or {},
                    "replace_profile_ids": replay_scope.get("replace_profile_ids") or [],
                    "incremental": bool(replay_scope.get("incremental")),
                },
            )
            result["replay_snapshot_id"] = replay_snapshot.get("snapshot_id")
            if use_event_archive:
                replay_context_path.write_text(
                    json.dumps(
                        {
                            "archive_source": "events",
                            "replay_mode": replay_mode,
                            "archive_entries_used": len(archive_entries),
                            "replay_snapshot_id": replay_snapshot.get("snapshot_id"),
                            "event_reconstruction_diagnostics": result.get("event_reconstruction_diagnostics") or {},
                        },
                        indent=2,
                    )
                )
            import_result = import_journeys_from_cdp_fn(
                req=from_cdp_request_factory(
                    config_id=payload.config_id,
                    import_note=payload.import_note or "Reprocessed from webhook archive",
                    replay_snapshot_id=replay_snapshot.get("snapshot_id"),
                ),
                db=db,
            )
            result["persisted_to_attribution"] = True
            result["import_result"] = import_result
            if use_event_archive:
                result["event_reconstruction_diagnostics"] = _build_event_replay_reconstruction_diagnostics(
                    archive_entries=archive_entries,
                    archived_profiles=archived_profiles,
                    import_result=import_result,
                )
                if replay_context_path.exists():
                    replay_context_path.write_text(
                        json.dumps(
                            {
                                "archive_source": "events",
                                "replay_mode": replay_mode,
                                "archive_entries_used": len(archive_entries),
                                "replay_snapshot_id": replay_snapshot.get("snapshot_id"),
                                "event_reconstruction_diagnostics": result.get("event_reconstruction_diagnostics") or {},
                            },
                            indent=2,
                        )
                    )
        return result

    @router.get("/api/connectors/meiro/webhook/suggestions")
    def meiro_webhook_suggestions(limit: int = Query(100, ge=1, le=500), db=Depends(get_db_dependency)):
        events = get_webhook_events(limit=limit)
        current_pull_config = get_pull_config()
        event_archive_entries = _get_event_archive_entries(db, limit=max(25, min(limit, 100)))
        raw_event_diagnostics = _build_raw_event_stream_diagnostics(
            event_archive_entries=event_archive_entries,
            webhook_events=events,
        )
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
        raw_events_analyzed = 0

        def _merge_counts(target: Dict[str, int], incoming: Dict[str, Any]) -> None:
            for key, value in (incoming or {}).items():
                try:
                    count = int(value)
                except Exception:
                    continue
                if key:
                    target[key] = target.get(key, 0) + count

        raw_event_profiles: List[Dict[str, Any]] = []
        for entry in event_archive_entries:
            if not isinstance(entry, dict):
                continue
            entry_events = entry.get("events") or []
            if isinstance(entry_events, list):
                raw_events_analyzed += len(entry_events)
                raw_event_profiles.extend(
                    rebuild_profiles_from_meiro_events_fn(entry_events, dedup_config=current_pull_config)
                )
        raw_event_analysis = _analyze_payload(raw_event_profiles) if raw_event_profiles else None
        stored_payload_analyses = [
            event.get("payload_analysis")
            for event in events
            if isinstance(event, dict) and isinstance(event.get("payload_analysis"), dict)
        ]
        analysis_sources = [raw_event_analysis] if isinstance(raw_event_analysis, dict) and int(raw_event_analysis.get("touchpoint_count") or 0) > 0 else stored_payload_analyses

        for analysis in analysis_sources:
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

        def _normalize_token(value: Optional[str]) -> str:
            return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")

        def _canonical_conversion_name(event_name: str) -> Optional[str]:
            token = _normalize_token(event_name)
            if not token:
                return None
            if token in {"purchase", "purchased", "order_completed", "order_complete", "order_paid", "checkout_completed"}:
                return "purchase"
            if token in {"lead", "lead_submitted", "form_submit", "form_submitted", "qualified_lead"}:
                return "lead"
            if token in {"signup", "sign_up", "registered", "register", "registration_completed"}:
                return "signup"
            return None

        top_conversion_names = _top_items(conversion_event_counts, n=10)
        top_value_fields = _top_items(value_field_counts, n=5)
        top_currency_fields = _top_items(currency_field_counts, n=5)
        top_sources = _top_items(source_counts, n=15)
        top_mediums = _top_items(medium_counts, n=15)
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

        sanitation_suggestions: List[Dict[str, Any]] = []
        sanitation_seen: set[str] = set()

        def _push_sanitation_suggestion(item: Dict[str, Any]) -> None:
            suggestion_id = str(item.get("id") or "")
            if not suggestion_id or suggestion_id in sanitation_seen:
                return
            sanitation_seen.add(suggestion_id)
            sanitation_suggestions.append(item)

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

        current_aliases = {
            str(key or "").strip().lower(): str(value or "").strip().lower()
            for key, value in (current_pull_config.get("conversion_event_aliases") or {}).items()
            if key and value
        }
        conversion_alias_suggestions: List[Dict[str, Any]] = []
        for event_name, count in top_conversion_names:
            canonical = _canonical_conversion_name(event_name)
            normalized_event = _normalize_token(event_name)
            if not canonical or not normalized_event or canonical == normalized_event:
                continue
            if current_aliases.get(normalized_event) == canonical:
                continue
            conversion_alias_suggestions.append(
                {
                    "raw_event": normalized_event,
                    "canonical_event": canonical,
                    "count": count,
                }
            )
            _push_sanitation_suggestion(
                {
                    "id": f"conversion_alias:{normalized_event}:{canonical}",
                    "type": "conversion_alias",
                    "title": f"Alias conversion '{normalized_event}' to '{canonical}'",
                    "description": "Observed webhook conversion event looks like a naming variant of a more stable canonical conversion.",
                    "impact_count": count,
                    "confidence": {"score": 0.78, "band": "medium"},
                    "recommended_action": "Add the alias before deterministic conversion mapping to reduce conversion drift.",
                    "payload": {"conversion_event_aliases": {normalized_event: canonical}},
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

        missing_pair_count = sum(
            count
            for pair_key, count in top_source_medium_pairs
            if not (pair_key.split("||", 1) + [""])[:2][0] or not (pair_key.split("||", 1) + [""])[:2][1]
        )
        unresolved_pair_count = sum(int(item.get("count") or 0) for item in unresolved_pairs)
        unresolved_share = (unresolved_pair_count / total_touchpoints) if total_touchpoints > 0 else 0.0
        missing_pair_share = (missing_pair_count / total_touchpoints) if total_touchpoints > 0 else 0.0
        best_dedup_coverage = (dedup_key_candidates[0]["coverage_pct"] / 100.0) if dedup_key_candidates else 0.0

        if unresolved_share >= 0.08 and not bool(current_pull_config.get("quarantine_unknown_channels", True)):
            _push_sanitation_suggestion(
                {
                    "id": "policy:quarantine_unknown_channels",
                    "type": "quarantine_policy",
                    "title": "Enable quarantine for unresolved channels",
                    "description": "A meaningful share of observed touchpoints still resolves to unknown taxonomy patterns.",
                    "impact_count": unresolved_pair_count,
                    "confidence": {"score": 0.84, "band": "high"},
                    "recommended_action": "Turn on unknown-channel quarantine until taxonomy aliases and rules catch up.",
                    "payload": {"quarantine_unknown_channels": True},
                }
            )

        if missing_pair_share >= 0.05 and not bool(current_pull_config.get("quarantine_missing_utm", False)):
            _push_sanitation_suggestion(
                {
                    "id": "policy:quarantine_missing_utm",
                    "type": "quarantine_policy",
                    "title": "Quarantine touchpoints missing source or medium",
                    "description": "Webhook drift is producing a material share of touchpoints without usable source/medium context.",
                    "impact_count": missing_pair_count,
                    "confidence": {"score": 0.81, "band": "high"},
                    "recommended_action": "Enable missing-UTM quarantine to keep incomplete records out of production journeys.",
                    "payload": {"quarantine_missing_utm": True},
                }
            )

        if total_conversions > 0 and not top_value_fields and current_pull_config.get("value_fallback_policy") != "quarantine":
            _push_sanitation_suggestion(
                {
                    "id": "policy:value_fallback_quarantine",
                    "type": "fallback_policy",
                    "title": "Quarantine conversions with missing values",
                    "description": "No stable numeric value field was observed in recent webhook conversions.",
                    "impact_count": total_conversions,
                    "confidence": {"score": 0.72, "band": "medium"},
                    "recommended_action": "Switch value fallback to quarantine until a reliable value field is mapped.",
                    "payload": {"value_fallback_policy": "quarantine"},
                }
            )

        if total_conversions > 0 and not top_currency_fields and current_pull_config.get("currency_fallback_policy") != "quarantine":
            _push_sanitation_suggestion(
                {
                    "id": "policy:currency_fallback_quarantine",
                    "type": "fallback_policy",
                    "title": "Quarantine conversions with missing currencies",
                    "description": "No stable currency field was observed in recent webhook conversions.",
                    "impact_count": total_conversions,
                    "confidence": {"score": 0.72, "band": "medium"},
                    "recommended_action": "Switch currency fallback to quarantine until currency mapping is explicit.",
                    "payload": {"currency_fallback_policy": "quarantine"},
                }
            )

        if best_dedup_coverage < 0.7 and current_pull_config.get("quarantine_duplicate_profiles") is not True:
            _push_sanitation_suggestion(
                {
                    "id": "policy:quarantine_duplicate_profiles",
                    "type": "dedupe_policy",
                    "title": "Quarantine duplicate profiles while dedupe coverage is weak",
                    "description": "Observed dedupe keys have weak coverage across recent conversion payloads.",
                    "impact_count": total_conversions,
                    "confidence": {"score": 0.7, "band": "medium"},
                    "recommended_action": "Enable duplicate-profile quarantine until a stronger dedupe key is consistently present.",
                    "payload": {"quarantine_duplicate_profiles": True},
                }
            )

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
        sanitation_apply_payload = {
            **current_pull_config,
            "conversion_event_aliases": {
                **current_aliases,
                **{
                    item["raw_event"]: item["canonical_event"]
                    for item in conversion_alias_suggestions
                    if item.get("raw_event") and item.get("canonical_event")
                },
            },
        }
        for suggestion in sanitation_suggestions:
            payload = suggestion.get("payload") or {}
            if isinstance(payload, dict):
                sanitation_apply_payload.update(payload)

        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "events_analyzed": raw_events_analyzed or len(events),
            "analysis_source": "event_archive" if raw_events_analyzed else "webhook_history",
            "total_conversions_observed": total_conversions,
            "total_touchpoints_observed": total_touchpoints,
            "event_stream_diagnostics": raw_event_diagnostics,
            "dedup_key_suggestion": best_dedup_key,
            "dedup_key_candidates": dedup_key_candidates,
            "kpi_suggestions": kpi_suggestions,
            "conversion_event_suggestions": [{"event_name": name, "count": count} for name, count in top_conversion_names],
            "sanitation_suggestions": sanitation_suggestions[:8],
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
                "sanitation": sanitation_apply_payload,
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

    @router.get("/api/connectors/meiro/auto-replay")
    def meiro_auto_replay_status(db=Depends(get_db_dependency)):
        return {
            "config": get_pull_config(),
            "state": get_auto_replay_state(),
            "history": _get_auto_replay_history_items(db, limit=25),
            "event_archive_status": _get_event_archive_status(db),
            "mapping_approval": (get_mapping_state().get("approval") or {}),
        }

    @router.post("/api/connectors/meiro/auto-replay/run")
    def meiro_auto_replay_run(
        trigger: str = Body(default="manual", embed=True),
        db=Depends(get_db_dependency),
    ):
        return _run_auto_replay(db, trigger=str(trigger or "manual").strip().lower() or "manual")

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
        except ValueError as exc:
            append_import_run_fn("meiro_pull", 0, "error", error=str(exc))
            raise HTTPException(status_code=409, detail=str(exc)) from exc
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
    def meiro_dry_run(limit: int = 100, db=Depends(get_db_dependency)):
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
        pull_cfg = get_pull_config()
        replay_mode = str(pull_cfg.get("replay_mode") or "last_n").strip().lower()
        if replay_mode not in {"all", "last_n", "date_range"}:
            replay_mode = "last_n"
        replay_archive_source = str(pull_cfg.get("replay_archive_source") or "auto").strip().lower()
        archive_limit = int(pull_cfg.get("replay_archive_limit") or 5000)
        date_from = pull_cfg.get("replay_date_from") if replay_mode == "date_range" else None
        date_to = pull_cfg.get("replay_date_to") if replay_mode == "date_range" else None
        archive_entries, archived_profiles, use_event_archive, _ = _load_archived_profiles_for_replay(
            db,
            replay_mode=replay_mode,
            archive_source=replay_archive_source,
            archive_limit=archive_limit,
            date_from=date_from,
            date_to=date_to,
        )
        profiles: list[Dict[str, Any]]
        if archived_profiles:
            profiles = archived_profiles
        elif cdp_json_path.exists():
            profiles = json.loads(cdp_json_path.read_text())
        elif cdp_path.exists():
            df = pd.read_csv(cdp_path)
            profiles = df.to_dict(orient="records")
        else:
            raise HTTPException(status_code=404, detail="No archived replay data or CDP data found.")
        profiles = profiles if isinstance(profiles, list) else []
        if not profiles:
            return {"count": 0, "preview": [], "warnings": ["No profiles to process"], "validation": {}}
        try:
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
        summary = result.get("import_summary") or {}
        cleaning_report = summary.get("cleaning_report") or {}
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
                "quality_score": (((journey.get("meta") or {}).get("quality") or {}).get("score")),
                "quality_band": (((journey.get("meta") or {}).get("quality") or {}).get("band")),
            }
            for journey in journeys[:20]
        ]
        return {
            "count": len(journeys),
            "preview": preview,
            "warnings": warnings,
            "validation": {"ok": len(warnings) == 0},
            "import_summary": summary,
            "cleaning_report": cleaning_report,
            "quarantine_count": int(summary.get("quarantined", 0) or 0),
            "archive_source": "events" if archived_profiles and use_event_archive else ("profiles" if archived_profiles else "cdp"),
            "archive_entries_used": len(archive_entries),
        }

    if register_auto_replay_runner_fn:
        register_auto_replay_runner_fn(_run_auto_replay)

    return router
