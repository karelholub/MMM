from __future__ import annotations

from typing import Any, Dict, List, Optional


def _confidence(score: float) -> Dict[str, Any]:
    bounded = max(0.0, min(100.0, float(score)))
    band = "high" if bounded >= 80 else "medium" if bounded >= 55 else "low"
    return {"score": round(bounded, 1), "band": band}


def _action(
    action_id: str,
    label: str,
    *,
    benefit: Optional[str] = None,
    target_tab: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "id": action_id,
        "label": label,
        "benefit": benefit,
        "domain": "meiro",
        "target_page": "meiro",
        "target_tab": target_tab,
        "requires_review": True,
    }


def build_meiro_readiness(
    *,
    meiro_connected: bool,
    meiro_config: Dict[str, Any],
    mapping_state: Dict[str, Any],
    archive_status: Dict[str, Any],
    event_archive_status: Optional[Dict[str, Any]] = None,
    pull_config: Dict[str, Any],
    raw_event_diagnostics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    approval = (mapping_state.get("approval") or {}) if isinstance(mapping_state, dict) else {}
    approval_status = str(approval.get("status") or "unreviewed").strip().lower()
    profile_webhook_received_count = int(meiro_config.get("webhook_received_count") or 0)
    event_webhook_received_count = int(meiro_config.get("event_webhook_received_count") or 0)
    webhook_has_secret = bool(meiro_config.get("webhook_has_secret"))
    profile_archive_entries = int(archive_status.get("entries") or 0)
    event_archive_status = event_archive_status or {}
    event_archive_entries = int(event_archive_status.get("entries") or 0)
    archive_entries = profile_archive_entries + event_archive_entries
    conversion_selector = str(pull_config.get("conversion_selector") or "").strip()
    primary_ingest_source = str(
        pull_config.get("primary_ingest_source")
        or meiro_config.get("primary_ingest_source")
        or "profiles"
    ).strip().lower() or "profiles"
    replay_archive_source = str(pull_config.get("replay_archive_source") or "auto").strip().lower() or "auto"
    dual_ingest_detected = profile_webhook_received_count > 0 and event_webhook_received_count > 0
    raw_event_diagnostics = raw_event_diagnostics or {}

    blockers: List[str] = []
    warnings: List[str] = []
    reasons: List[str] = []
    actions: List[Dict[str, Any]] = []

    has_any_ingestion_path = meiro_connected or profile_webhook_received_count > 0 or event_webhook_received_count > 0
    if not has_any_ingestion_path:
        blockers.append("Neither Meiro CDP pull nor Meiro Pipes webhook ingestion is active.")
        actions.append(
            _action(
                "connect_meiro_source",
                "Connect CDP pull or configure Pipes webhook",
                benefit="Enable Meiro to provide journeys into attribution",
                target_tab="cdp",
            )
        )
    else:
        reasons.append("At least one Meiro ingestion path is active.")

    if meiro_connected:
        reasons.append("Meiro CDP connection is configured.")
    else:
        if primary_ingest_source == "events" and (event_webhook_received_count > 0 or event_archive_entries > 0):
            reasons.append("Meiro CDP pull is not connected; raw Pipes events are the active attribution source.")
        else:
            warnings.append("Meiro CDP pull is not connected.")
            actions.append(
                _action(
                    "connect_cdp_pull",
                    "Connect Meiro CDP pull",
                    benefit="Enable manual and scheduled audience API imports",
                    target_tab="cdp",
                )
            )

    if profile_webhook_received_count > 0:
        reasons.append(f"Meiro Pipes profiles webhook has delivered {profile_webhook_received_count} payloads.")
    if event_webhook_received_count > 0:
        reasons.append(f"Meiro Pipes raw events webhook has delivered {event_webhook_received_count} events.")
    if profile_webhook_received_count == 0 and event_webhook_received_count == 0:
        warnings.append("No Meiro Pipes webhook payloads have been received yet.")
        actions.append(
            _action(
                "configure_pipes_webhook",
                "Configure Meiro Pipes webhook",
                benefit="Enable push-based ingestion and replay from archived payloads",
                target_tab="pipes",
            )
        )

    if primary_ingest_source == "events":
        if event_webhook_received_count <= 0 and event_archive_entries <= 0:
            warnings.append("Primary Meiro source is set to raw events, but no raw-event traffic has been received yet.")
        else:
            reasons.append("Raw events are configured as the primary Meiro attribution source.")
        if raw_event_diagnostics.get("available"):
            if float(raw_event_diagnostics.get("usable_event_name_share") or 0.0) < 0.6:
                warnings.append("Raw-event naming quality is still weak for attribution.")
            if float(raw_event_diagnostics.get("source_medium_share") or 0.0) < 0.3:
                warnings.append("Raw-event source/medium coverage is still low.")
            if float(raw_event_diagnostics.get("conversion_linkage_share") or 0.0) < 0.6 and int(raw_event_diagnostics.get("conversion_like_events") or 0) > 0:
                warnings.append("Raw-event conversion linkage coverage is still low.")
    else:
        if profile_webhook_received_count <= 0 and profile_archive_entries <= 0:
            warnings.append("Primary Meiro source is set to profiles, but no profile payloads have been received yet.")
        else:
            reasons.append("Profiles are configured as the primary Meiro attribution source.")

    if dual_ingest_detected:
        warnings.append("Both Meiro profile and raw-event webhooks are active. Only one should be treated as the primary attribution source.")
        actions.append(
            _action(
                "review_meiro_primary_source",
                "Choose a single primary Meiro source",
                benefit="Avoid overlapping profile and raw-event attribution inputs",
                target_tab="pipes",
            )
        )
        if replay_archive_source == "auto":
            warnings.append("Replay source is set to auto while both Meiro webhooks are active.")
            actions.append(
                _action(
                    "pin_meiro_replay_source",
                    "Pin replay source instead of auto",
                    benefit="Keep replays aligned with the intended primary Meiro source",
                    target_tab="pipes",
                )
            )

    if not webhook_has_secret:
        warnings.append("Webhook secret is not configured for Meiro Pipes.")
        actions.append(
            _action(
                "rotate_webhook_secret",
                "Generate a webhook secret",
                benefit="Protect Meiro Pipes ingestion from unauthorized payloads",
                target_tab="pipes",
            )
        )

    if approval_status != "approved":
        if approval_status == "rejected":
            blockers.append("Current Meiro normalization mapping is rejected.")
        else:
            warnings.append("Current Meiro normalization mapping is not approved.")
        actions.append(
            _action(
                "review_meiro_mapping",
                "Review and approve Meiro normalization mapping",
                benefit="Make replay and import decisions safer and more consistent",
                target_tab="normalization",
            )
        )
    else:
        reasons.append("Current Meiro mapping is approved.")

    if archive_entries > 0:
        warnings.append(f"{archive_entries} archived Meiro webhook batches are available for replay.")
        actions.append(
            _action(
                "review_replay_backlog",
                "Review replay backlog",
                benefit="Recover archived Meiro Pipes payloads into attribution",
                target_tab="import",
            )
        )
    else:
        reasons.append("No archived replay backlog is waiting.")

    if not conversion_selector:
        warnings.append("Pull config does not define a conversion selector.")
        actions.append(
            _action(
                "review_pull_config",
                "Review CDP pull config",
                benefit="Keep pulled Meiro conversions aligned with measurement settings",
                target_tab="cdp",
            )
        )
    else:
        reasons.append(f"CDP pull config uses conversion selector '{conversion_selector}'.")

    status = "ready"
    score = 88.0
    if blockers:
        status = "blocked"
        score = 28.0
    elif warnings:
        status = "warning"
        score = 62.0

    deduped_actions: List[Dict[str, Any]] = []
    seen = set()
    for action in actions:
        action_id = action.get("id")
        if not action_id or action_id in seen:
            continue
        seen.add(action_id)
        deduped_actions.append(action)

    return {
        "status": status,
        "confidence": _confidence(score),
        "summary": {
            "cdp_connected": meiro_connected,
            "webhook_received_count": profile_webhook_received_count,
            "event_webhook_received_count": event_webhook_received_count,
            "webhook_has_secret": webhook_has_secret,
            "mapping_status": approval_status or "unreviewed",
            "mapping_version": int(mapping_state.get("version") or 0) if isinstance(mapping_state, dict) else 0,
            "archive_entries": archive_entries,
            "profile_archive_entries": profile_archive_entries,
            "event_archive_entries": event_archive_entries,
            "last_test_at": meiro_config.get("last_test_at"),
            "last_webhook_received_at": meiro_config.get("webhook_last_received_at") or archive_status.get("last_received_at"),
            "last_event_webhook_received_at": meiro_config.get("event_webhook_last_received_at") or event_archive_status.get("last_received_at"),
            "conversion_selector": conversion_selector or None,
            "primary_ingest_source": primary_ingest_source,
            "replay_archive_source": replay_archive_source,
            "dual_ingest_detected": dual_ingest_detected,
            "raw_event_diagnostics": raw_event_diagnostics,
        },
        "blockers": blockers,
        "warnings": warnings,
        "reasons": reasons,
        "recommended_actions": deduped_actions,
    }
