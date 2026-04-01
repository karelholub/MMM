from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models_config_dq import ConversionPath
from .services_canonical_facts import resolve_canonical_history_bounds
from .services_journey_definitions import list_active_journey_definitions
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
from .modules.settings.schemas import KpiConfigModel


def _resolve_default_reprocess_days(db: Session) -> int:
    active = get_active_journey_settings(db, use_cache=True)
    return max(
        1,
        int(
            ((active.get("settings_json") or {}).get("performance_guardrails") or {}).get(
                "aggregation_reprocess_window_days",
                3,
            )
            or 3
        ),
    )


def _resolve_effective_reprocess_days(db: Session, requested_reprocess_days: Optional[int] = None) -> int:
    return max(1, int(requested_reprocess_days or _resolve_default_reprocess_days(db)))


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
    history_bounds = resolve_canonical_history_bounds(db)
    if not history_bounds or not history_bounds[0] or not history_bounds[1]:
        history_bounds = db.query(
            func.min(ConversionPath.conversion_ts),
            func.max(ConversionPath.conversion_ts),
        ).one_or_none()
    history_reprocess = 1
    if history_bounds:
        history_start, history_end = history_bounds
        if history_start and history_end:
            history_reprocess = max(1, (history_end.date() - history_start.date()).days + 1)
    effective_reprocess = max(_resolve_effective_reprocess_days(db, reprocess_days), history_reprocess)
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
    effective_reprocess_days = _resolve_effective_reprocess_days(db, reprocess_days)
    metrics = rebuild_journey_definition_outputs_from_daily(
        db,
        definition_id=definition_id,
        reprocess_days=effective_reprocess_days,
    )
    return {
        **metrics,
        "effective_reprocess_days": effective_reprocess_days,
    }


def rebuild_multiple_journey_definition_outputs(
    db: Session,
    *,
    definition_ids: Optional[list[str]] = None,
    reprocess_days: Optional[int] = None,
) -> Dict[str, Any]:
    active_definitions = (
        list_active_journey_definitions(db)
        if definition_ids is None
        else list_active_journey_definitions(db)
    )
    if definition_ids is not None:
        wanted = {str(definition_id).strip() for definition_id in definition_ids if str(definition_id).strip()}
        active_definitions = [definition for definition in active_definitions if definition.id in wanted]

    effective_reprocess_days = _resolve_effective_reprocess_days(db, reprocess_days)
    results = [
        rebuild_journey_definition_outputs_from_daily(
            db,
            definition_id=definition.id,
            reprocess_days=effective_reprocess_days,
        )
        for definition in active_definitions
    ]
    return {
        "definitions_rebuilt": len(results),
        "definition_ids": [definition.id for definition in active_definitions],
        "effective_reprocess_days": effective_reprocess_days,
        "results": results,
        "days_processed": sum(int(result.get("days_processed") or 0) for result in results),
        "source_rows_processed": sum(int(result.get("source_rows_processed") or 0) for result in results),
        "path_rows_written": sum(int(result.get("path_rows_written") or 0) for result in results),
        "transition_rows_written": sum(int(result.get("transition_rows_written") or 0) for result in results),
        "example_rows_written": sum(int(result.get("example_rows_written") or 0) for result in results),
        "definition_rows_written": sum(int(result.get("definition_rows_written") or 0) for result in results),
        "obsolete_days_removed": sum(int(result.get("obsolete_days_removed") or 0) for result in results),
    }


def rebuild_outputs_for_taxonomy_change(
    db: Session,
    *,
    taxonomy: Optional[Taxonomy] = None,
    reprocess_days: Optional[int] = None,
) -> Dict[str, Any]:
    return {
        "taxonomy": rebuild_taxonomy_dq_outputs(db, taxonomy=taxonomy),
        "aggregate_outputs": rebuild_journey_aggregate_outputs(db, reprocess_days=reprocess_days),
        "journey_outputs": rebuild_multiple_journey_definition_outputs(db, reprocess_days=reprocess_days),
    }


def rebuild_outputs_for_kpi_config_change(
    db: Session,
    *,
    previous_cfg: KpiConfigModel,
    current_cfg: KpiConfigModel,
    reprocess_days: Optional[int] = None,
) -> Dict[str, Any]:
    def _as_signature(definition: Any) -> Dict[str, Any]:
        if hasattr(definition, "model_dump"):
            return definition.model_dump()
        if hasattr(definition, "dict"):
            return definition.dict()
        return dict(definition or {})

    previous_by_id = {item.id: _as_signature(item) for item in previous_cfg.definitions}
    current_by_id = {item.id: _as_signature(item) for item in current_cfg.definitions}
    impacted_kpi_ids = {
        kpi_id
        for kpi_id in set(previous_by_id) | set(current_by_id)
        if previous_by_id.get(kpi_id) != current_by_id.get(kpi_id)
    }
    if (previous_cfg.primary_kpi_id or "") != (current_cfg.primary_kpi_id or ""):
        if previous_cfg.primary_kpi_id:
            impacted_kpi_ids.add(previous_cfg.primary_kpi_id)
        if current_cfg.primary_kpi_id:
            impacted_kpi_ids.add(current_cfg.primary_kpi_id)

    affected_definitions = list_active_journey_definitions(db, conversion_kpi_ids=impacted_kpi_ids)
    rebuild = rebuild_multiple_journey_definition_outputs(
        db,
        definition_ids=[definition.id for definition in affected_definitions],
        reprocess_days=reprocess_days,
    )
    aggregate_outputs = rebuild_journey_aggregate_outputs(db, reprocess_days=reprocess_days)
    return {
        "impacted_kpi_ids": sorted(impacted_kpi_ids),
        "affected_definition_ids": [definition.id for definition in affected_definitions],
        "aggregate_outputs": aggregate_outputs,
        "rebuild": rebuild,
    }


def purge_journey_definition_outputs(
    db: Session,
    *,
    definition_id: str,
) -> Dict[str, Any]:
    return purge_journey_definition_outputs_from_daily(db, definition_id=definition_id)
