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
    pull_config: Dict[str, Any],
) -> Dict[str, Any]:
    approval = (mapping_state.get("approval") or {}) if isinstance(mapping_state, dict) else {}
    approval_status = str(approval.get("status") or "unreviewed").strip().lower()
    webhook_received_count = int(meiro_config.get("webhook_received_count") or 0)
    webhook_has_secret = bool(meiro_config.get("webhook_has_secret"))
    archive_entries = int(archive_status.get("entries") or 0)
    conversion_selector = str(pull_config.get("conversion_selector") or "").strip()

    blockers: List[str] = []
    warnings: List[str] = []
    reasons: List[str] = []
    actions: List[Dict[str, Any]] = []

    has_any_ingestion_path = meiro_connected or webhook_received_count > 0
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
        warnings.append("Meiro CDP pull is not connected.")
        actions.append(
            _action(
                "connect_cdp_pull",
                "Connect Meiro CDP pull",
                benefit="Enable manual and scheduled audience API imports",
                target_tab="cdp",
            )
        )

    if webhook_received_count > 0:
        reasons.append(f"Meiro Pipes has delivered {webhook_received_count} payloads.")
    else:
        warnings.append("No Meiro Pipes webhook payloads have been received yet.")
        actions.append(
            _action(
                "configure_pipes_webhook",
                "Configure Meiro Pipes webhook",
                benefit="Enable push-based ingestion and replay from archived payloads",
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
        warnings.append(f"{archive_entries} archived webhook batches are available for replay.")
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
            "webhook_received_count": webhook_received_count,
            "webhook_has_secret": webhook_has_secret,
            "mapping_status": approval_status or "unreviewed",
            "mapping_version": int(mapping_state.get("version") or 0) if isinstance(mapping_state, dict) else 0,
            "archive_entries": archive_entries,
            "last_test_at": meiro_config.get("last_test_at"),
            "last_webhook_received_at": meiro_config.get("webhook_last_received_at") or archive_status.get("last_received_at"),
            "conversion_selector": conversion_selector or None,
        },
        "blockers": blockers,
        "warnings": warnings,
        "reasons": reasons,
        "recommended_actions": deduped_actions,
    }
