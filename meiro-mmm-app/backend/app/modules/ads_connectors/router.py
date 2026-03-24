from typing import Any, Callable, Optional

from fastapi import APIRouter


def create_router(
    *,
    connectors_status_fn: Callable[[], Any],
    fetch_meta_fn: Callable[..., Any],
    fetch_google_fn: Callable[..., Any],
    fetch_linkedin_fn: Callable[..., Any],
    merge_ads_fn: Callable[[], Any],
) -> APIRouter:
    router = APIRouter(tags=["ads_connectors"])

    @router.get("/api/connectors/status")
    def connectors_status():
        return connectors_status_fn()

    @router.post("/api/connectors/meta")
    def fetch_meta(
        ad_account_id: str,
        since: str,
        until: str,
        avg_aov: float = 0.0,
        access_token: Optional[str] = None,
    ):
        return fetch_meta_fn(ad_account_id=ad_account_id, since=since, until=until, avg_aov=avg_aov, access_token=access_token)

    @router.post("/api/connectors/google")
    def fetch_google(segments_date_from: str, segments_date_to: str):
        return fetch_google_fn(segments_date_from=segments_date_from, segments_date_to=segments_date_to)

    @router.post("/api/connectors/linkedin")
    def fetch_linkedin(since: str, until: str, access_token: Optional[str] = None):
        return fetch_linkedin_fn(since=since, until=until, access_token=access_token)

    @router.post("/api/connectors/merge")
    def merge_ads():
        return merge_ads_fn()

    return router
