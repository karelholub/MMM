"""Shared CDP import workflow for Meiro profile and replay snapshot ingestion."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional
import uuid

import pandas as pd
from fastapi import HTTPException

from app.services_meiro_replay_snapshots import get_meiro_replay_snapshot
from app.utils.meiro_config import filter_journeys_to_target_site_scope, get_target_site_domains, site_scope_is_strict


def import_journeys_from_cdp_source(
    *,
    req: Any,
    db: Any,
    data_dir: Path,
    get_mapping_fn: Callable[[], Dict[str, Any]],
    attribution_mapping_config_cls: Any,
    get_pull_config_fn: Callable[[], Dict[str, Any]],
    get_settings_obj: Callable[[], Any],
    canonicalize_meiro_profiles_fn: Callable[..., Dict[str, Any]],
    get_default_config_id_fn: Callable[[], Optional[str]],
    get_model_config_fn: Callable[[Any, str], Any],
    apply_model_config_fn: Callable[[list[dict], dict], list[dict]],
    create_quarantine_run_fn: Callable[..., Any],
    persist_journeys_fn: Callable[..., Any],
    refresh_journey_aggregates_fn: Callable[..., Any],
    append_import_run_fn: Callable[..., Any],
    set_active_journey_source_fn: Callable[[str], None],
    journey_revenue_value_fn: Callable[[dict], float],
) -> Dict[str, Any]:
    cdp_json_path = data_dir / "meiro_cdp_profiles.json"
    cdp_path = data_dir / "meiro_cdp.csv"
    replay_context_path = data_dir / "meiro_replay_context.json"
    replay_context: Dict[str, Any] = {}
    if replay_context_path.exists():
        try:
            replay_context = json.loads(replay_context_path.read_text(encoding="utf-8"))
        except Exception:
            replay_context = {}
    replay_context_active = bool(replay_context)

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

    replay_snapshot_id = getattr(req, "replay_snapshot_id", None)
    replay_snapshot = get_meiro_replay_snapshot(db, replay_snapshot_id) if replay_snapshot_id else None
    source_label = "meiro_events_replay" if str(replay_context.get("archive_source") or "") == "events" else "meiro_webhook"
    if replay_snapshot_id and not replay_snapshot:
        append_import_run_fn("meiro_events_replay", 0, "error", error=f"Replay snapshot {replay_snapshot_id} not found.")
        raise HTTPException(status_code=404, detail="Replay snapshot not found.")
    if replay_snapshot:
        profiles = replay_snapshot.get("profiles_json") or []
        source_label = "meiro_events_replay" if str(replay_snapshot.get("source_kind") or "") == "events" else source_label
    elif cdp_json_path.exists():
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
        if replay_context_active and replay_context_path.exists():
            try:
                replay_context_path.unlink()
            except Exception:
                pass
        append_import_run_fn(source_label, 0, "error", error=str(exc))
        raise
    journeys_before_site_scope = len(journeys)
    journeys = filter_journeys_to_target_site_scope(journeys, allow_unknown=True)
    journeys_excluded_by_site_scope = journeys_before_site_scope - len(journeys)

    effective_config_id = getattr(req, "config_id", None) or get_default_config_id_fn()
    if effective_config_id:
        cfg = get_model_config_fn(db, effective_config_id)
        if cfg:
            journeys = apply_model_config_fn(journeys, cfg.config_json or {})

    quarantine_records = result.get("quarantine_records") or []
    cleaning_report = ((result.get("import_summary") or {}).get("cleaning_report") or {})
    import_batch_id = str(uuid.uuid4())
    source_snapshot_id = replay_snapshot.get("snapshot_id") if replay_snapshot else None
    attributable_profile_ids = {
        str(
            profile.get("customer_id")
            or ((profile.get("customer") or {}) if isinstance(profile.get("customer"), dict) else {}).get("id")
            or ""
        ).strip()
        for profile in profiles
        if isinstance(profile, dict)
        and bool(profile.get("touchpoints"))
        and bool(profile.get("conversions"))
        and str(
            profile.get("customer_id")
            or ((profile.get("customer") or {}) if isinstance(profile.get("customer"), dict) else {}).get("id")
            or ""
        ).strip()
    }
    replace_profile_ids = []
    if replay_snapshot:
        snapshot_context = replay_snapshot.get("context_json") or {}
        replace_profile_ids = [
            str(profile_id).strip()
            for profile_id in (snapshot_context.get("replace_profile_ids") or [])
            if str(profile_id).strip()
        ]
    if quarantine_records:
        create_quarantine_run_fn(
            source=source_label,
            import_note=getattr(req, "import_note", None),
            parser_version=result.get("schema_version"),
            summary=cleaning_report,
            records=quarantine_records,
        )

    persist_journeys_fn(
        db,
        journeys,
        replace=True,
        replace_profile_ids=replace_profile_ids,
        import_source=source_label,
        import_batch_id=import_batch_id,
        source_snapshot_id=source_snapshot_id,
    )
    refresh_journey_aggregates_fn(db)
    converted = sum(
        1
        for journey in journeys
        if bool(journey.get("conversions")) or bool(journey.get("converted", False))
    )
    persisted_profile_ids = {
        str(
            journey.get("customer_id")
            or ((journey.get("customer") or {}) if isinstance(journey.get("customer"), dict) else {}).get("id")
            or ""
        ).strip()
        for journey in journeys
        if isinstance(journey, dict)
        and str(
            journey.get("customer_id")
            or ((journey.get("customer") or {}) if isinstance(journey.get("customer"), dict) else {}).get("id")
            or ""
        ).strip()
    }
    persisted_attributable_profile_count = len(persisted_profile_ids & attributable_profile_ids)
    channels_detected = sorted(
        {
            touchpoint.get("channel", "unknown")
            for journey in journeys
            for touchpoint in journey.get("touchpoints", [])
        }
    )
    summary = result.get("import_summary") or {}
    pull_cfg = get_pull_config_fn()
    validation_summary = {
        "cleaning_report": cleaning_report,
        "top_quarantine_reasons": cleaning_report.get("top_unresolved_patterns") or [],
        "site_scope": {
            "strict": site_scope_is_strict(),
            "target_sites": get_target_site_domains(),
            "journeys_before_scope": journeys_before_site_scope,
            "journeys_excluded": journeys_excluded_by_site_scope,
        },
    }
    if replay_context:
        validation_summary["replay_context"] = replay_context
        if replay_context.get("event_reconstruction_diagnostics"):
            validation_summary["event_reconstruction_diagnostics"] = replay_context.get("event_reconstruction_diagnostics")
    config_snapshot = {
        "conversion_import_batch_id": import_batch_id,
        "mapping_preset": "saved",
        "schema_version": "1.0",
        "dedup_interval_minutes": pull_cfg.get("dedup_interval_minutes"),
        "dedup_mode": pull_cfg.get("dedup_mode"),
        "primary_dedup_key": pull_cfg.get("primary_dedup_key"),
        "fallback_dedup_keys": pull_cfg.get("fallback_dedup_keys"),
        "strict_ingest": pull_cfg.get("strict_ingest"),
        "site_scope_strict": site_scope_is_strict(),
        "target_site_domains": get_target_site_domains(),
        "journeys_excluded_by_site_scope": journeys_excluded_by_site_scope,
        "timestamp_fallback_policy": pull_cfg.get("timestamp_fallback_policy"),
        "value_fallback_policy": pull_cfg.get("value_fallback_policy"),
        "currency_fallback_policy": pull_cfg.get("currency_fallback_policy"),
    }
    if replay_context:
        config_snapshot["replay_context"] = replay_context
    if replay_snapshot:
        config_snapshot["replay_snapshot_id"] = replay_snapshot.get("snapshot_id")
        config_snapshot["replay_snapshot_context"] = replay_snapshot.get("context_json") or {}
        config_snapshot["source_snapshot_id"] = source_snapshot_id
        if replace_profile_ids:
            config_snapshot["replace_profile_ids"] = replace_profile_ids
    append_import_run_fn(
        source_label,
        len(journeys),
        "success",
        total=int(summary.get("total", len(journeys)) or len(journeys)),
        valid=len(journeys),
        invalid=len(quarantine_records),
        converted=converted,
        channels_detected=channels_detected,
        validation_summary=validation_summary,
        config_snapshot=config_snapshot,
        preview_rows=[
            {
                "customer_id": journey.get("customer_id", "?"),
                "touchpoints": len(journey.get("touchpoints", [])),
                "value": journey_revenue_value_fn(journey),
            }
            for journey in journeys[:20]
        ],
        import_note=getattr(req, "import_note", None),
    )
    if replay_context_active and replay_context_path.exists():
        try:
            replay_context_path.unlink()
        except Exception:
            pass
    set_active_journey_source_fn("meiro")
    return {
        "count": len(journeys),
        "message": f"Parsed {len(journeys)} journeys from CDP data",
        "import_summary": summary,
        "quarantine_count": len(quarantine_records),
        "persisted_profile_count": len(persisted_profile_ids),
        "persisted_attributable_profile_count": persisted_attributable_profile_count,
    }
