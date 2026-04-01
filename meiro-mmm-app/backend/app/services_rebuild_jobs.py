from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import ConversionPath
from .services_journey_aggregates import (
    purge_journey_definition_outputs as purge_journey_definition_outputs_from_daily,
    rebuild_journey_definition_outputs as rebuild_journey_definition_outputs_from_daily,
    run_daily_journey_aggregates,
)
from .services_journey_settings import get_active_journey_settings
from .services_taxonomy import (
    backfill_taxonomy_dq_snapshots_from_db,
    persist_taxonomy_dq_snapshots,
    persist_taxonomy_dq_snapshots_from_db,
)
from .utils.taxonomy import Taxonomy


def rebuild_taxonomy_dq_outputs(
    db: Session,
    *,
    taxonomy: Optional[Taxonomy] = None,
) -> Dict[str, Any]:
    backfill = backfill_taxonomy_dq_snapshots_from_db(db, taxonomy=taxonomy)
    snapshots = persist_taxonomy_dq_snapshots_from_db(db, taxonomy=taxonomy)
    source = "db_touchpoint_facts"
    if snapshots is None:
        source = "journeys_fallback"
        from .services_conversions import load_journeys_from_db

        journeys = load_journeys_from_db(db, limit=10000)
        snapshots = persist_taxonomy_dq_snapshots(db, journeys, taxonomy=taxonomy)
    return {
        "backfill": backfill,
        "snapshots": snapshots,
        "source": source,
    }


def rebuild_journey_aggregate_outputs(
    db: Session,
    *,
    reprocess_days: Optional[int] = None,
) -> Dict[str, Any]:
    active = get_active_journey_settings(db, use_cache=True)
    default_reprocess = (
        ((active.get("settings_json") or {}).get("performance_guardrails") or {}).get(
            "aggregation_reprocess_window_days",
            3,
        )
    )
    history_bounds = db.query(
        func.min(ConversionPath.conversion_ts),
        func.max(ConversionPath.conversion_ts),
    ).one_or_none()
    history_reprocess = 1
    if history_bounds:
        history_start, history_end = history_bounds
        if history_start and history_end:
            history_reprocess = max(1, (history_end.date() - history_start.date()).days + 1)
    effective_reprocess = max(1, int(reprocess_days or default_reprocess or 3), history_reprocess)
    metrics = run_daily_journey_aggregates(db, reprocess_days=effective_reprocess)
    return {
        **metrics,
        "effective_reprocess_days": effective_reprocess,
    }


def rebuild_journey_definition_outputs(
    db: Session,
    *,
    definition_id: str,
    reprocess_days: Optional[int] = None,
) -> Dict[str, Any]:
    active = get_active_journey_settings(db, use_cache=True)
    default_reprocess = (
        ((active.get("settings_json") or {}).get("performance_guardrails") or {}).get(
            "aggregation_reprocess_window_days",
            3,
        )
    )
    metrics = rebuild_journey_definition_outputs_from_daily(
        db,
        definition_id=definition_id,
        reprocess_days=max(1, int(reprocess_days or default_reprocess or 3)),
    )
    return {
        **metrics,
        "effective_reprocess_days": max(1, int(reprocess_days or default_reprocess or 3)),
    }


def purge_journey_definition_outputs(
    db: Session,
    *,
    definition_id: str,
) -> Dict[str, Any]:
    return purge_journey_definition_outputs_from_daily(db, definition_id=definition_id)
