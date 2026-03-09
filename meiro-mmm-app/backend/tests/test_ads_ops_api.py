import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.connectors.ads_ops.base import AdsApplyResult, AdsProviderError
from app.db import Base, get_db
from app.main import app
from app.services_access_control import ensure_access_control_seed_data


class _FakeAdsAdapter:
    key = "google_ads"

    def build_deep_link(self, *, account_id: str, entity_type: str, entity_id: str) -> str:
        return f"https://ads.example.com/{account_id}/{entity_type}/{entity_id}"

    def fetch_entity_state(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str):
        return {"status": "enabled", "budget": 100.0, "currency": "USD", "name": entity_id}

    def pause_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str):
        return AdsApplyResult(ok=True, provider_request_id=f"pause-{idempotency_key}")

    def enable_entity(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, idempotency_key: str):
        return AdsApplyResult(ok=True, provider_request_id=f"enable-{idempotency_key}")

    def update_budget(self, *, access_token: str, account_id: str, entity_type: str, entity_id: str, daily_budget: float, currency: str | None, idempotency_key: str):
        return AdsApplyResult(ok=True, provider_request_id=f"budget-{idempotency_key}")

    def normalize_error(self, exc: Exception):
        return AdsProviderError(code="provider_error", message=str(exc), retryable=False, needs_reauth=False)

    def supports(self, action_type: str, entity_type: str) -> bool:
        return action_type in {"pause", "enable", "update_budget"}


@pytest.fixture
def client(monkeypatch):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        ensure_access_control_seed_data(db)
    finally:
        db.close()

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    monkeypatch.setattr("app.services_ads_ops.get_ads_adapter", lambda _provider: _FakeAdsAdapter())
    monkeypatch.setattr("app.services_ads_ops.get_access_token_for_provider", lambda *_args, **_kwargs: "token-1")

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    engine.dispose()


def _admin_headers():
    return {"X-User-Role": "admin", "X-User-Id": "qa-admin"}


def _viewer_headers():
    return {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}


def test_ads_deeplink_endpoint_returns_url(client: TestClient):
    resp = client.get(
        "/api/ads/deeplink",
        params={
            "provider": "google_ads",
            "account_id": "acct-1",
            "entity_type": "campaign",
            "entity_id": "cmp-7",
        },
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["url"].endswith("/acct-1/campaign/cmp-7")


def test_create_approve_apply_change_request_and_audit(client: TestClient):
    created = client.post(
        "/api/ads/change-requests",
        json={
            "provider": "google_ads",
            "account_id": "acct-1",
            "entity_type": "campaign",
            "entity_id": "cmp-9",
            "action_type": "pause",
            "action_payload": {},
        },
        headers=_admin_headers(),
    )
    assert created.status_code == 200
    request_id = created.json()["id"]
    assert created.json()["status"] == "pending_approval"

    approved = client.post(f"/api/ads/change-requests/{request_id}/approve", headers=_admin_headers())
    assert approved.status_code == 200
    assert approved.json()["status"] == "approved"

    applied = client.post(f"/api/ads/change-requests/{request_id}/apply", json={"admin_override": False}, headers=_admin_headers())
    assert applied.status_code == 200
    assert applied.json()["status"] == "applied"

    audit = client.get("/api/ads/audit", headers=_admin_headers())
    assert audit.status_code == 200
    events = {row["event_type"] for row in audit.json()["items"]}
    assert "proposal_created" in events
    assert "approved" in events
    assert "applied" in events


def test_viewer_cannot_propose_or_apply_ads_changes(client: TestClient):
    create_resp = client.post(
        "/api/ads/change-requests",
        json={
            "provider": "google_ads",
            "account_id": "acct-1",
            "entity_type": "campaign",
            "entity_id": "cmp-10",
            "action_type": "pause",
            "action_payload": {},
        },
        headers=_viewer_headers(),
    )
    assert create_resp.status_code == 403

    # Viewer can still read entities endpoints with ads.view.
    read_resp = client.get(
        "/api/ads/state",
        params={
            "provider": "google_ads",
            "account_id": "acct-1",
            "entity_type": "campaign",
            "entity_id": "cmp-10",
        },
        headers=_viewer_headers(),
    )
    assert read_resp.status_code == 200
