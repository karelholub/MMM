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
    target_page: str = "datasources",
) -> Dict[str, Any]:
    return {
        "id": action_id,
        "label": label,
        "benefit": benefit,
        "domain": "data_sources",
        "target_page": target_page,
        "requires_review": True,
    }


def build_data_sources_readiness(
    *,
    data_sources: List[Dict[str, Any]],
    import_health: Dict[str, Any],
    meiro_readiness: Dict[str, Any],
    journeys_loaded: int,
) -> Dict[str, Any]:
    blockers: List[str] = []
    warnings: List[str] = []
    reasons: List[str] = []
    actions: List[Dict[str, Any]] = []

    connected_sources = [
        row for row in data_sources
        if str(row.get("status") or "").lower() not in {"", "not_connected", "disabled"}
    ]
    warehouse_sources = [row for row in data_sources if row.get("category") == "warehouse"]
    ad_platform_sources = [row for row in data_sources if row.get("category") == "ad_platform"]
    import_sources = import_health.get("sources") or []
    overall_import_freshness = str(import_health.get("overall_freshness") or "unknown")
    healthy_import_sources = sum(1 for row in import_sources if row.get("status") == "Healthy")
    syncing_sources = sum(1 for row in import_sources if row.get("syncing"))
    meiro_status = str(meiro_readiness.get("status") or "unknown")

    if journeys_loaded <= 0:
        blockers.append("No journeys are currently loaded into the workspace.")
        actions.append(
            _action(
                "load_journeys",
                "Load journeys into the workspace",
                benefit="Enable attribution, journeys analysis, and quality checks",
            )
        )
    else:
        reasons.append(f"{journeys_loaded} journeys are currently available in the workspace.")

    if meiro_status == "blocked":
        blockers.append("Meiro integration is not operational yet.")
        actions.append(
            _action(
                "review_meiro_workspace",
                "Review Meiro workspace",
                benefit="Restore the primary ingestion path for CDP and Pipes data",
                target_page="meiro",
            )
        )
    elif meiro_status == "warning":
        warnings.append("Meiro integration still needs review before it can be fully trusted.")
        actions.append(
            _action(
                "review_meiro_warnings",
                "Review Meiro operational warnings",
                benefit="Tighten ingestion, mapping, and replay readiness",
                target_page="meiro",
            )
        )
    else:
        reasons.append("Meiro integration is in a usable state.")

    if overall_import_freshness in {"Broken", "Stale"}:
        warnings.append(f"Ad-platform import freshness is currently {overall_import_freshness.lower()}.")
        actions.append(
            _action(
                "review_import_health",
                "Review ad-platform import health",
                benefit="Restore fresh spend and performance inputs",
            )
        )
    elif overall_import_freshness == "Partial":
        warnings.append("At least one import source has only partially completed.")
    else:
        reasons.append(f"Overall import freshness is {overall_import_freshness.lower()}.")

    if not connected_sources:
        warnings.append("No warehouse or ad-platform sources are connected.")
        actions.append(
            _action(
                "connect_sources",
                "Connect data sources",
                benefit="Improve import coverage and operational redundancy",
            )
        )
    else:
        reasons.append(f"{len(connected_sources)} connected data sources are available.")

    status = "ready"
    score = 86.0
    if blockers:
        status = "blocked"
        score = 28.0
    elif warnings:
        status = "warning"
        score = 61.0

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
            "journeys_loaded": journeys_loaded,
            "connected_sources": len(connected_sources),
            "warehouse_sources": len(warehouse_sources),
            "ad_platform_sources": len(ad_platform_sources),
            "overall_import_freshness": overall_import_freshness,
            "healthy_import_sources": healthy_import_sources,
            "syncing_sources": syncing_sources,
            "meiro_status": meiro_status,
        },
        "blockers": blockers,
        "warnings": warnings,
        "reasons": reasons,
        "recommended_actions": deduped_actions,
    }
