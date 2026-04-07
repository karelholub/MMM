from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.connectors import meiro_cdp


@pytest.fixture
def client():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides.clear()
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    engine.dispose()


def test_local_segment_crud_and_registry(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(meiro_cdp, "is_connected", lambda: True)
    monkeypatch.setattr(
        meiro_cdp,
        "list_segments",
        lambda: [{"id": "seg-1", "name": "VIP buyers", "profiles_count": 42}],
    )

    headers_view = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}
    headers_edit = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}

    create_resp = client.post(
        "/api/segments/local",
        headers=headers_edit,
        json={
            "name": "Mobile CZ paid",
            "description": "Local analytical segment",
            "definition": {
                "channel_group": "paid_search",
                "device": "mobile",
                "country": "cz",
                "ignored_key": "x",
            },
        },
    )
    assert create_resp.status_code == 200
    created = create_resp.json()
    assert created["name"] == "Mobile CZ paid"
    assert created["source"] == "local_analytical"
    assert created["definition"] == {
        "channel_group": "paid_search",
        "device": "mobile",
        "country": "cz",
    }
    segment_id = created["id"]

    registry_resp = client.get("/api/segments/registry", headers=headers_view)
    assert registry_resp.status_code == 200
    registry = registry_resp.json()
    assert registry["summary"]["local_analytical"] == 1
    assert registry["summary"]["meiro_pipes"] == 1
    assert any(item["id"] == segment_id for item in registry["items"])
    assert any(item["source"] == "meiro_pipes" for item in registry["items"])

    update_resp = client.put(
        f"/api/segments/local/{segment_id}",
        headers=headers_edit,
        json={
            "name": "Desktop CZ paid",
            "description": "Updated segment",
            "definition": {
                "channel_group": "paid_search",
                "device": "desktop",
                "country": "cz",
            },
        },
    )
    assert update_resp.status_code == 200
    assert update_resp.json()["definition"]["device"] == "desktop"

    archive_resp = client.post(f"/api/segments/local/{segment_id}/archive", headers=headers_edit)
    assert archive_resp.status_code == 200
    assert archive_resp.json()["status"] == "archived"

    active_list = client.get("/api/segments/local", headers=headers_view)
    assert active_list.status_code == 200
    assert active_list.json()["items"] == []

    all_list = client.get("/api/segments/local?include_archived=true", headers=headers_view)
    assert all_list.status_code == 200
    assert len(all_list.json()["items"]) == 1

    restore_resp = client.post(f"/api/segments/local/{segment_id}/restore", headers=headers_edit)
    assert restore_resp.status_code == 200
    assert restore_resp.json()["status"] == "active"
