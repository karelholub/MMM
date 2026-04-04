from typing import Any, Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.permissions import PermissionContext
from app.modules.ads_governance.schemas import (
    AdsChangeRequestApplyPayload,
    BudgetRecommendationBulkCreatePayload,
    AdsChangeRequestCreatePayload,
    AdsChangeRequestRejectPayload,
)
from app.services_ads_ops import create_change_requests_from_budget_targets


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[..., Any],
    workspace_scope_or_403_fn: Callable[..., str],
    get_ads_governance_settings_fn: Callable[[], Any],
    ads_provider_keys_obj: Any,
    ads_entity_types_obj: Any,
    ads_list_entities_fn: Callable[..., Any],
    ads_get_deep_link_fn: Callable[..., str],
    ads_fetch_state_fn: Callable[..., Any],
    ads_create_change_request_fn: Callable[..., Any],
    ads_list_change_requests_fn: Callable[..., Any],
    ads_approve_change_request_fn: Callable[..., Any],
    ads_reject_change_request_fn: Callable[..., Any],
    ads_apply_change_request_fn: Callable[..., Any],
    ads_list_audit_fn: Callable[..., Any],
) -> APIRouter:
    router = APIRouter(tags=["ads_governance"])

    @router.get("/api/ads/entities")
    def get_ads_entities(
        provider: Optional[str] = Query(default=None),
        entity_type: Optional[str] = Query(default=None),
        search: Optional[str] = Query(default=None),
        limit: int = Query(default=100, ge=1, le=500),
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.view")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        try:
            if provider and provider not in ads_provider_keys_obj:
                raise ValueError("Unsupported provider")
            if entity_type and entity_type not in ads_entity_types_obj:
                raise ValueError("Unsupported entity_type")
            return ads_list_entities_fn(
                db,
                workspace_id=workspace_id,
                provider=provider,
                entity_type=entity_type,
                search=search,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/ads/deeplink")
    def get_ads_deeplink(
        provider: str = Query(...),
        account_id: Optional[str] = Query(default=None),
        entity_type: str = Query(...),
        entity_id: str = Query(..., min_length=1),
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.view")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        try:
            url = ads_get_deep_link_fn(
                db,
                workspace_id=workspace_id,
                provider=provider,
                account_id=account_id,
                entity_type=entity_type,
                entity_id=entity_id,
            )
            return {"provider": provider, "entity_type": entity_type, "entity_id": entity_id, "url": url}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/ads/state")
    def get_ads_state(
        provider: str = Query(...),
        account_id: str = Query(..., min_length=1),
        entity_type: str = Query(...),
        entity_id: str = Query(..., min_length=1),
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.view")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        try:
            return ads_fetch_state_fn(
                db,
                workspace_id=workspace_id,
                provider=provider,
                account_id=account_id,
                entity_type=entity_type,
                entity_id=entity_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/api/ads/change-requests")
    def create_ads_change_request(
        payload: AdsChangeRequestCreatePayload,
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.propose")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        governance = get_ads_governance_settings_fn()
        try:
            return ads_create_change_request_fn(
                db,
                workspace_id=workspace_id,
                requested_by_user_id=ctx.user_id,
                provider=payload.provider,
                account_id=payload.account_id,
                entity_type=payload.entity_type,
                entity_id=payload.entity_id,
                action_type=payload.action_type,
                action_payload=payload.action_payload,
                approval_required=bool(governance.require_approval),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/api/ads/change-requests/from-budget-recommendation")
    def create_ads_change_requests_from_budget_recommendation(
        payload: BudgetRecommendationBulkCreatePayload,
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.propose")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        governance = get_ads_governance_settings_fn()
        try:
            return create_change_requests_from_budget_targets(
                db,
                workspace_id=workspace_id,
                requested_by_user_id=ctx.user_id,
                run_id=payload.run_id,
                scenario_id=payload.scenario_id,
                recommendation_id=payload.recommendation_id,
                currency=payload.currency,
                targets=[target.model_dump() for target in payload.targets],
                approval_required=bool(governance.require_approval),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/ads/change-requests")
    def get_ads_change_requests(
        status: Optional[str] = Query(default=None),
        provider: Optional[str] = Query(default=None),
        limit: int = Query(default=200, ge=1, le=1000),
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.view")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        try:
            if status and status not in {"draft", "pending_approval", "approved", "rejected", "applied", "failed", "cancelled"}:
                raise ValueError("Unsupported status")
            if provider and provider not in ads_provider_keys_obj:
                raise ValueError("Unsupported provider")
            return ads_list_change_requests_fn(
                db,
                workspace_id=workspace_id,
                status=status,
                provider=provider,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/api/ads/change-requests/{request_id}/approve")
    def approve_ads_change_request(
        request_id: str,
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.apply")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        try:
            return ads_approve_change_request_fn(
                db,
                workspace_id=workspace_id,
                request_id=request_id,
                actor_user_id=ctx.user_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/api/ads/change-requests/{request_id}/reject")
    def reject_ads_change_request(
        request_id: str,
        payload: AdsChangeRequestRejectPayload,
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.apply")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        try:
            return ads_reject_change_request_fn(
                db,
                workspace_id=workspace_id,
                request_id=request_id,
                actor_user_id=ctx.user_id,
                reason=payload.reason,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/api/ads/change-requests/{request_id}/apply")
    def apply_ads_change_request(
        request_id: str,
        payload: AdsChangeRequestApplyPayload,
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.apply")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        governance = get_ads_governance_settings_fn()
        try:
            return ads_apply_change_request_fn(
                db,
                workspace_id=workspace_id,
                request_id=request_id,
                actor_user_id=ctx.user_id,
                require_approval=bool(governance.require_approval),
                budget_change_limit_pct=max(0.0, float(governance.max_budget_change_pct)),
                admin_override=bool(payload.admin_override),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/ads/audit")
    def get_ads_audit(
        provider: Optional[str] = Query(default=None),
        entity_id: Optional[str] = Query(default=None),
        limit: int = Query(default=200, ge=1, le=1000),
        db=Depends(get_db_dependency),
        ctx: PermissionContext = Depends(require_permission_dependency("ads.view")),
    ):
        workspace_id = workspace_scope_or_403_fn(ctx, None)
        try:
            if provider and provider not in ads_provider_keys_obj:
                raise ValueError("Unsupported provider")
            return ads_list_audit_fn(
                db,
                workspace_id=workspace_id,
                provider=provider,
                entity_id=entity_id,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return router
