import logging
from datetime import datetime
from typing import Any, Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.modules.journeys.schemas import (
    JourneyDefinitionCreate,
    JourneyExperimentCreatePayload,
    JourneyDefinitionUpdate,
    JourneyHypothesisPayload,
    JourneyPolicyPromotionPayload,
    JourneyPolicySimulationPayload,
    JourneySavedViewPayload,
)
from app.models_config_dq import JourneyHypothesis
from app.services_journey_attribution import build_journey_attribution_summary
from app.services_journey_definitions import (
    archive_journey_definition,
    create_journey_definition,
    get_journey_definition,
    list_journey_definitions,
    serialize_journey_definition,
    update_journey_definition,
)
from app.services_journey_examples import list_examples_for_journey_definition
from app.services_journey_hypotheses import (
    create_journey_hypothesis,
    list_journey_hypotheses,
    serialize_journey_hypothesis,
    update_journey_hypothesis,
)
from app.services_journey_insights import build_journey_insights
from app.services_journey_paths import list_paths_for_journey_definition
from app.services_journey_policy_learning import (
    list_journey_policy_candidates,
    serialize_journey_policy_candidate_response,
    set_journey_policy_promotion,
)
from app.services_journey_saved_views import (
    create_journey_saved_view,
    delete_journey_saved_view,
    list_journey_saved_views,
    serialize_journey_saved_view,
    update_journey_saved_view,
)
from app.services_journey_transitions import list_transitions_for_journey_definition
from app.services_incrementality import create_experiment_record, serialize_experiment_summary
from app.services_nba_policy_simulation import build_journey_policy_simulation

logger = logging.getLogger(__name__)


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    clamp_int_fn: Callable[..., int],
    resolve_per_page_fn: Callable[..., int],
    resolve_sort_dir_fn: Callable[..., str],
    validate_conversion_kpi_id_fn: Callable[[Optional[str]], Optional[str]],
    get_settings_obj: Callable[[], Any],
    rebuild_journey_definition_outputs_fn: Callable[..., dict],
    purge_journey_definition_outputs_fn: Callable[..., dict],
) -> APIRouter:
    router = APIRouter(tags=["journeys"])

    @router.get("/api/journeys/definitions")
    def api_list_journey_definitions(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: Optional[int] = Query(None, description="Items per page"),
        page_size: Optional[int] = Query(None, description="Alias for per_page"),
        limit: Optional[int] = Query(None, description="Alias for per_page"),
        search: Optional[str] = Query(None, description="Search by name/description"),
        sort: Optional[str] = Query(None, description="Sort direction asc|desc"),
        order: Optional[str] = Query(None, description="Alias for sort direction asc|desc"),
        include_archived: bool = Query(False, description="Include archived definitions"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        resolved_page = clamp_int_fn(page, default=1, minimum=1, maximum=1_000_000)
        resolved_per_page = resolve_per_page_fn(
            per_page=per_page,
            page_size=page_size,
            limit=limit,
            default=20,
            maximum=100,
        )
        resolved_sort_dir = resolve_sort_dir_fn(sort=sort, order=order, default="desc")
        try:
            return list_journey_definitions(
                db,
                page=resolved_page,
                per_page=resolved_per_page,
                search=search,
                sort_dir=resolved_sort_dir,
                include_archived=include_archived,
            )
        except Exception as exc:
            logger.warning("List journey definitions failed: %s", exc, exc_info=True)
            return {"items": [], "total": 0, "page": resolved_page, "per_page": resolved_per_page}

    @router.get("/api/journeys/views")
    def api_list_journey_saved_views(
        journey_definition_id: Optional[str] = Query(None),
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        return list_journey_saved_views(
            db,
            workspace_id=ctx.workspace_id,
            user_id=ctx.user_id,
            journey_definition_id=journey_definition_id,
        )

    @router.post("/api/journeys/views")
    def api_create_journey_saved_view(
        body: JourneySavedViewPayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        if not body.name.strip():
            raise HTTPException(status_code=400, detail="name is required")
        try:
            item = create_journey_saved_view(
                db,
                workspace_id=ctx.workspace_id,
                user_id=ctx.user_id,
                journey_definition_id=body.journey_definition_id,
                name=body.name,
                state=body.state,
                actor_user_id=ctx.user_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return serialize_journey_saved_view(item)

    @router.put("/api/journeys/views/{view_id}")
    def api_update_journey_saved_view(
        view_id: str,
        body: JourneySavedViewPayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        if not body.name.strip():
            raise HTTPException(status_code=400, detail="name is required")
        try:
            item = update_journey_saved_view(
                db,
                view_id=view_id,
                workspace_id=ctx.workspace_id,
                user_id=ctx.user_id,
                name=body.name,
                state=body.state,
                journey_definition_id=body.journey_definition_id,
                actor_user_id=ctx.user_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        if not item:
            raise HTTPException(status_code=404, detail="Journey saved view not found")
        return serialize_journey_saved_view(item)

    @router.delete("/api/journeys/views/{view_id}")
    def api_delete_journey_saved_view(
        view_id: str,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        ok = delete_journey_saved_view(
            db,
            view_id=view_id,
            workspace_id=ctx.workspace_id,
            user_id=ctx.user_id,
        )
        if not ok:
            raise HTTPException(status_code=404, detail="Journey saved view not found")
        return {"id": view_id, "status": "deleted"}

    @router.get("/api/journeys/hypotheses")
    def api_list_journey_hypotheses(
        journey_definition_id: Optional[str] = Query(None),
        status: Optional[str] = Query(None),
        mine_only: bool = Query(False),
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        return list_journey_hypotheses(
            db,
            workspace_id=ctx.workspace_id,
            journey_definition_id=journey_definition_id,
            status=status,
            owner_user_id=ctx.user_id if mine_only else None,
        )

    @router.post("/api/journeys/hypotheses")
    def api_create_journey_hypothesis(
        body: JourneyHypothesisPayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        jd = get_journey_definition(db, body.journey_definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        if not body.title.strip():
            raise HTTPException(status_code=400, detail="title is required")
        if not body.hypothesis_text.strip():
            raise HTTPException(status_code=400, detail="hypothesis_text is required")
        item = create_journey_hypothesis(
            db,
            workspace_id=ctx.workspace_id,
            owner_user_id=ctx.user_id,
            journey_definition_id=body.journey_definition_id,
            title=body.title,
            target_kpi=body.target_kpi,
            hypothesis_text=body.hypothesis_text,
            trigger=body.trigger,
            segment=body.segment,
            current_action=body.current_action,
            proposed_action=body.proposed_action,
            support_count=body.support_count,
            baseline_rate=body.baseline_rate,
            sample_size_target=body.sample_size_target,
            status=body.status,
            linked_experiment_id=body.linked_experiment_id,
            result=body.result,
        )
        return serialize_journey_hypothesis(item)

    @router.put("/api/journeys/hypotheses/{hypothesis_id}")
    def api_update_journey_hypothesis(
        hypothesis_id: str,
        body: JourneyHypothesisPayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        jd = get_journey_definition(db, body.journey_definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        if not body.title.strip():
            raise HTTPException(status_code=400, detail="title is required")
        if not body.hypothesis_text.strip():
            raise HTTPException(status_code=400, detail="hypothesis_text is required")
        item = update_journey_hypothesis(
            db,
            workspace_id=ctx.workspace_id,
            hypothesis_id=hypothesis_id,
            title=body.title,
            target_kpi=body.target_kpi,
            hypothesis_text=body.hypothesis_text,
            trigger=body.trigger,
            segment=body.segment,
            current_action=body.current_action,
            proposed_action=body.proposed_action,
            support_count=body.support_count,
            baseline_rate=body.baseline_rate,
            sample_size_target=body.sample_size_target,
            status=body.status,
            linked_experiment_id=body.linked_experiment_id,
            result=body.result,
        )
        if not item:
            raise HTTPException(status_code=404, detail="Journey hypothesis not found")
        return serialize_journey_hypothesis(item)

    @router.post("/api/journeys/hypotheses/{hypothesis_id}/create-experiment")
    def api_create_experiment_from_journey_hypothesis(
        hypothesis_id: str,
        body: JourneyExperimentCreatePayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        if body.start_at >= body.end_at:
            raise HTTPException(status_code=400, detail="start_at must be before end_at")
        hypothesis = (
            db.query(JourneyHypothesis)
            .filter(JourneyHypothesis.id == hypothesis_id, JourneyHypothesis.workspace_id == ctx.workspace_id)
            .first()
        )
        if not hypothesis:
            raise HTTPException(status_code=404, detail="Journey hypothesis not found")
        if hypothesis.linked_experiment_id:
            raise HTTPException(status_code=400, detail="Journey hypothesis already linked to an experiment")
        jd = get_journey_definition(db, hypothesis.journey_definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")

        derived_channel = (
            (body.channel or "").strip()
            or str((hypothesis.segment_json or {}).get("channel_group") or "").strip()
            or "journey"
        )
        proposed_action = dict(hypothesis.proposed_action_json or {})
        if body.proposed_step and body.proposed_step.strip():
            proposed_action["step"] = body.proposed_step.strip()
            proposed_action["type"] = str(proposed_action.get("type") or "nba_intervention")
            proposed_action["idea"] = str(
                proposed_action.get("idea")
                or f"Test {body.proposed_step.strip()} for this prefix."
            )
        experiment_name = (body.name or "").strip() or f"Journey test: {hypothesis.title}"
        note_parts = [
            body.notes.strip() if body.notes else "",
            f"Created from journey hypothesis {hypothesis.id}.",
            f"Journey definition: {hypothesis.journey_definition_id}.",
            f"Hypothesis: {hypothesis.hypothesis_text}",
        ]
        experiment = create_experiment_record(
            db,
            name=experiment_name,
            channel=derived_channel,
            start_at=body.start_at,
            end_at=body.end_at,
            conversion_key=hypothesis.target_kpi or jd.conversion_kpi_id,
            notes="\n\n".join(part for part in note_parts if part),
            experiment_type=(body.experiment_type or "holdout").strip() or "holdout",
            source_type="journey_hypothesis",
            source_id=hypothesis.id,
            segment=dict(hypothesis.segment_json or {}),
            policy={
                "trigger": dict(hypothesis.trigger_json or {}),
                "current_action": dict(hypothesis.current_action_json or {}),
                "proposed_action": proposed_action,
            },
            guardrails={
                "sample_size_target": hypothesis.sample_size_target,
                "support_count": hypothesis.support_count,
                **(body.guardrails or {}),
            },
        )

        hypothesis.linked_experiment_id = experiment.id
        hypothesis.status = "in_experiment"
        hypothesis.proposed_action_json = proposed_action
        hypothesis.updated_at = datetime.utcnow()
        db.add(hypothesis)
        db.commit()
        db.refresh(hypothesis)
        return {
            "experiment": serialize_experiment_summary(
                experiment,
                source_name=hypothesis.title,
                source_journey_definition_id=hypothesis.journey_definition_id,
            ),
            "hypothesis": serialize_journey_hypothesis(hypothesis),
        }

    @router.post("/api/journeys/hypotheses/{hypothesis_id}/simulate")
    def api_simulate_journey_hypothesis_policy(
        hypothesis_id: str,
        body: Optional[JourneyPolicySimulationPayload] = None,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        hypothesis = (
            db.query(JourneyHypothesis)
            .filter(JourneyHypothesis.id == hypothesis_id, JourneyHypothesis.workspace_id == ctx.workspace_id)
            .first()
        )
        if not hypothesis:
            raise HTTPException(status_code=404, detail="Journey hypothesis not found")
        return build_journey_policy_simulation(
            db,
            hypothesis=hypothesis,
            proposed_step=(body.proposed_step if body else None),
        )

    @router.get("/api/journeys/{definition_id}/policies")
    def api_list_journey_policy_candidates(
        definition_id: str,
        limit: int = Query(12, ge=1, le=50),
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        jd = get_journey_definition(db, definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        return list_journey_policy_candidates(
            db,
            workspace_id=ctx.workspace_id,
            journey_definition_id=definition_id,
            limit=limit,
        )

    @router.post("/api/journeys/hypotheses/{hypothesis_id}/policy-promotion")
    def api_set_journey_policy_promotion(
        hypothesis_id: str,
        body: JourneyPolicyPromotionPayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        try:
            item = set_journey_policy_promotion(
                db,
                workspace_id=ctx.workspace_id,
                hypothesis_id=hypothesis_id,
                actor_user_id=ctx.user_id,
                active=body.active,
                notes=body.notes,
            )
        except ValueError as exc:
            message = str(exc)
            status_code = 404 if "not found" in message.lower() else 400
            raise HTTPException(status_code=status_code, detail=message)
        return serialize_journey_policy_candidate_response(item)

    @router.post("/api/journeys/definitions")
    def api_create_journey_definition(
        body: JourneyDefinitionCreate,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.manage")),
    ):
        user_id = ctx.user_id
        if not body.name.strip():
            raise HTTPException(status_code=400, detail="name is required")
        conversion_kpi_id = validate_conversion_kpi_id_fn(body.conversion_kpi_id)
        item = create_journey_definition(
            db,
            name=body.name,
            description=body.description,
            conversion_kpi_id=conversion_kpi_id,
            lookback_window_days=body.lookback_window_days,
            mode_default=body.mode_default,
            created_by=user_id,
        )
        try:
            rebuild_journey_definition_outputs_fn(db, definition_id=item.id)
        except Exception as exc:
            logger.warning("Journey definition rebuild after create failed: %s", exc, exc_info=True)
        return serialize_journey_definition(item)

    @router.put("/api/journeys/definitions/{definition_id}")
    def api_update_journey_definition(
        definition_id: str,
        body: JourneyDefinitionUpdate,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.manage")),
    ):
        user_id = ctx.user_id
        if not body.name.strip():
            raise HTTPException(status_code=400, detail="name is required")
        conversion_kpi_id = validate_conversion_kpi_id_fn(body.conversion_kpi_id)
        item = update_journey_definition(
            db,
            definition_id,
            name=body.name,
            description=body.description,
            conversion_kpi_id=conversion_kpi_id,
            lookback_window_days=body.lookback_window_days,
            mode_default=body.mode_default,
            updated_by=user_id,
        )
        if not item:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            rebuild_journey_definition_outputs_fn(db, definition_id=item.id)
        except Exception as exc:
            logger.warning("Journey definition rebuild after update failed: %s", exc, exc_info=True)
        return serialize_journey_definition(item)

    @router.delete("/api/journeys/definitions/{definition_id}")
    def api_delete_journey_definition(
        definition_id: str,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.manage")),
    ):
        user_id = ctx.user_id
        item = archive_journey_definition(db, definition_id, archived_by=user_id)
        if not item:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            purge_journey_definition_outputs_fn(db, definition_id=item.id)
        except Exception as exc:
            logger.warning("Journey definition output purge after archive failed: %s", exc, exc_info=True)
        return {"id": item.id, "status": "archived"}

    @router.post("/api/journeys/definitions/{definition_id}/rebuild")
    def api_rebuild_journey_definition(
        definition_id: str,
        reprocess_days: Optional[int] = Query(None, ge=1, le=365),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("journeys.manage")),
    ):
        item = get_journey_definition(db, definition_id)
        if not item or item.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            metrics = rebuild_journey_definition_outputs_fn(
                db,
                definition_id=definition_id,
                reprocess_days=reprocess_days,
            )
        except Exception as exc:
            logger.warning("Journey definition rebuild failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail="Journey definition rebuild failed")
        return {
            "definition_id": definition_id,
            "metrics": metrics,
        }

    @router.get("/api/journeys/{definition_id}/paths")
    def api_get_journey_paths(
        definition_id: str,
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        mode: str = Query("conversion_only", pattern="^(conversion_only|all_journeys)$"),
        channel_group: Optional[str] = Query(None),
        campaign_id: Optional[str] = Query(None),
        device: Optional[str] = Query(None),
        country: Optional[str] = Query(None),
        page: int = Query(1, ge=1, description="Page number"),
        limit: int = Query(50, ge=1, le=200, description="Items per page (max 200)"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        jd = get_journey_definition(db, definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            d_from = datetime.fromisoformat(date_from).date()
            d_to = datetime.fromisoformat(date_to).date()
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
        if d_from > d_to:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")

        return list_paths_for_journey_definition(
            db,
            journey_definition_id=definition_id,
            date_from=d_from,
            date_to=d_to,
            mode=mode,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            page=page,
            limit=limit,
        )

    @router.get("/api/journeys/{definition_id}/insights")
    def api_get_journey_insights(
        definition_id: str,
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        mode: str = Query("conversion_only", pattern="^(conversion_only|all_journeys)$"),
        channel_group: Optional[str] = Query(None),
        campaign_id: Optional[str] = Query(None),
        device: Optional[str] = Query(None),
        country: Optional[str] = Query(None),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        jd = get_journey_definition(db, definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            d_from = datetime.fromisoformat(date_from).date()
            d_to = datetime.fromisoformat(date_to).date()
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
        if d_from > d_to:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")
        return build_journey_insights(
            db,
            journey_definition_id=definition_id,
            date_from=d_from,
            date_to=d_to,
            mode=mode,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            target_kpi=jd.conversion_kpi_id,
        )

    @router.get("/api/journeys/{definition_id}/transitions")
    def api_get_journey_transitions(
        definition_id: str,
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        mode: str = Query("conversion_only", pattern="^(conversion_only|all_journeys)$"),
        channel_group: Optional[str] = Query(None),
        campaign_id: Optional[str] = Query(None),
        device: Optional[str] = Query(None),
        country: Optional[str] = Query(None),
        min_count: int = Query(5, ge=1, le=100000),
        max_nodes: int = Query(20, ge=2, le=200),
        max_depth: int = Query(5, ge=1, le=20),
        group_other: bool = Query(True, description="Group rare steps into 'Other'"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        jd = get_journey_definition(db, definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            d_from = datetime.fromisoformat(date_from).date()
            d_to = datetime.fromisoformat(date_to).date()
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
        if d_from > d_to:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")

        return list_transitions_for_journey_definition(
            db,
            journey_definition_id=definition_id,
            date_from=d_from,
            date_to=d_to,
            mode=mode,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            min_count=min_count,
            max_nodes=max_nodes,
            max_depth=max_depth,
            group_other=group_other,
        )

    @router.get("/api/journeys/{definition_id}/attribution-summary")
    def api_get_journey_attribution_summary(
        definition_id: str,
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        model: str = Query("linear", description="Attribution model"),
        mode: str = Query("conversion_only", pattern="^(conversion_only|all_journeys)$"),
        channel_group: Optional[str] = Query(None),
        campaign_id: Optional[str] = Query(None),
        device: Optional[str] = Query(None),
        country: Optional[str] = Query(None),
        path_hash: Optional[str] = Query(None),
        include_campaign: bool = Query(False, description="Include campaign-level credit split"),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        jd = get_journey_definition(db, definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            datetime.fromisoformat(date_from).date()
            datetime.fromisoformat(date_to).date()
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
        try:
            return build_journey_attribution_summary(
                db,
                definition=jd,
                date_from=date_from,
                date_to=date_to,
                model=model,
                mode=mode,
                channel_group=channel_group,
                campaign_id=campaign_id,
                device=device,
                country=country,
                path_hash=path_hash,
                include_campaign=include_campaign,
                settings_obj=get_settings_obj(),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @router.get("/api/journeys/{definition_id}/examples")
    def api_get_journey_examples(
        definition_id: str,
        date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
        date_to: str = Query(..., description="End date (YYYY-MM-DD)"),
        channel_group: Optional[str] = Query(None),
        campaign_id: Optional[str] = Query(None),
        device: Optional[str] = Query(None),
        country: Optional[str] = Query(None),
        path_hash: Optional[str] = Query(None),
        contains_step: Optional[str] = Query(None),
        limit: int = Query(12, ge=1, le=50),
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        jd = get_journey_definition(db, definition_id)
        if not jd or jd.is_archived:
            raise HTTPException(status_code=404, detail="Journey definition not found")
        try:
            d_from = datetime.fromisoformat(date_from).date()
            d_to = datetime.fromisoformat(date_to).date()
        except Exception:
            raise HTTPException(status_code=400, detail="date_from/date_to must be YYYY-MM-DD")
        if d_from > d_to:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")

        return list_examples_for_journey_definition(
            db,
            definition=jd,
            date_from=d_from,
            date_to=d_to,
            channel_group=channel_group,
            campaign_id=campaign_id,
            device=device,
            country=country,
            path_hash=path_hash,
            contains_step=contains_step,
            limit=limit,
        )

    return router
