from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Query

from app.modules.segments.schemas import LocalSegmentPayload
from app.services_segments import (
    create_local_segment,
    list_local_segments,
    list_segment_registry,
    set_local_segment_status,
    update_local_segment,
)


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
) -> APIRouter:
    router = APIRouter(tags=["segments"])

    @router.get("/api/segments/registry")
    def api_list_segment_registry(
        include_archived: bool = Query(False),
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        return list_segment_registry(
            db,
            workspace_id=ctx.workspace_id,
            include_archived=include_archived,
        )

    @router.get("/api/segments/local")
    def api_list_local_segments(
        include_archived: bool = Query(False),
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.view")),
    ):
        return {
            "items": list_local_segments(
                db,
                workspace_id=ctx.workspace_id,
                include_archived=include_archived,
            )
        }

    @router.post("/api/segments/local")
    def api_create_local_segment(
        body: LocalSegmentPayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.manage")),
    ):
        if not body.name.strip():
            raise HTTPException(status_code=400, detail="name is required")
        if not body.definition:
            raise HTTPException(status_code=400, detail="definition is required")
        return create_local_segment(
            db,
            workspace_id=ctx.workspace_id,
            owner_user_id=ctx.user_id,
            name=body.name,
            description=body.description,
            definition=body.definition,
        )

    @router.put("/api/segments/local/{segment_id}")
    def api_update_local_segment(
        segment_id: str,
        body: LocalSegmentPayload,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.manage")),
    ):
        if not body.name.strip():
            raise HTTPException(status_code=400, detail="name is required")
        if not body.definition:
            raise HTTPException(status_code=400, detail="definition is required")
        item = update_local_segment(
            db,
            segment_id=segment_id,
            workspace_id=ctx.workspace_id,
            name=body.name,
            description=body.description,
            definition=body.definition,
        )
        if not item:
            raise HTTPException(status_code=404, detail="Segment not found")
        return item

    @router.post("/api/segments/local/{segment_id}/archive")
    def api_archive_local_segment(
        segment_id: str,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.manage")),
    ):
        item = set_local_segment_status(
            db,
            segment_id=segment_id,
            workspace_id=ctx.workspace_id,
            status="archived",
        )
        if not item:
            raise HTTPException(status_code=404, detail="Segment not found")
        return item

    @router.post("/api/segments/local/{segment_id}/restore")
    def api_restore_local_segment(
        segment_id: str,
        db=Depends(get_db_dependency),
        ctx=Depends(require_permission_dependency("journeys.manage")),
    ):
        item = set_local_segment_status(
            db,
            segment_id=segment_id,
            workspace_id=ctx.workspace_id,
            status="active",
        )
        if not item:
            raise HTTPException(status_code=404, detail="Segment not found")
        return item

    return router
