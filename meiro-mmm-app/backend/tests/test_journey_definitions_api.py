from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app


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


def test_journey_definitions_crud_and_archive_flow(client: TestClient):
    create_resp = client.post(
        "/api/journeys/definitions",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Lifecycle journey",
            "description": "Core lifecycle journey",
            "conversion_kpi_id": "purchase",
            "lookback_window_days": 45,
            "mode_default": "conversion_only",
        },
    )
    assert create_resp.status_code == 200
    created = create_resp.json()
    assert created["name"] == "Lifecycle journey"
    assert created["conversion_kpi_id"] == "purchase"
    assert created["created_by"] == "qa-editor"
    definition_id = created["id"]

    list_resp = client.get("/api/journeys/definitions")
    assert list_resp.status_code == 200
    listed = list_resp.json()
    assert listed["total"] == 1
    assert listed["items"][0]["id"] == definition_id

    update_resp = client.put(
        f"/api/journeys/definitions/{definition_id}",
        headers={"X-User-Role": "admin", "X-User-Id": "qa-admin"},
        json={
            "name": "Lifecycle journey v2",
            "description": "Updated definition",
            "conversion_kpi_id": "lead",
            "lookback_window_days": 30,
            "mode_default": "all_journeys",
        },
    )
    assert update_resp.status_code == 200
    updated = update_resp.json()
    assert updated["name"] == "Lifecycle journey v2"
    assert updated["mode_default"] == "all_journeys"
    assert updated["updated_by"] == "qa-admin"

    delete_resp = client.delete(
        f"/api/journeys/definitions/{definition_id}",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
    )
    assert delete_resp.status_code == 200
    assert delete_resp.json()["status"] == "archived"

    list_active = client.get("/api/journeys/definitions")
    assert list_active.status_code == 200
    assert list_active.json()["total"] == 0

    list_all = client.get("/api/journeys/definitions", params={"include_archived": "true"})
    assert list_all.status_code == 200
    assert list_all.json()["total"] == 1
    assert list_all.json()["items"][0]["is_archived"] is True


def test_journey_definitions_list_search_and_sort(client: TestClient):
    headers = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}

    r1 = client.post(
        "/api/journeys/definitions",
        headers=headers,
        json={"name": "Alpha path", "conversion_kpi_id": "purchase", "lookback_window_days": 30, "mode_default": "conversion_only"},
    )
    assert r1.status_code == 200
    id1 = r1.json()["id"]

    r2 = client.post(
        "/api/journeys/definitions",
        headers=headers,
        json={"name": "Beta path", "conversion_kpi_id": "lead", "lookback_window_days": 30, "mode_default": "conversion_only"},
    )
    assert r2.status_code == 200
    id2 = r2.json()["id"]

    upd = client.put(
        f"/api/journeys/definitions/{id1}",
        headers=headers,
        json={"name": "Alpha path updated", "conversion_kpi_id": "purchase", "lookback_window_days": 31, "mode_default": "conversion_only"},
    )
    assert upd.status_code == 200

    desc = client.get("/api/journeys/definitions", params={"sort": "desc"})
    assert desc.status_code == 200
    assert desc.json()["items"][0]["id"] == id1

    asc = client.get("/api/journeys/definitions", params={"sort": "asc"})
    assert asc.status_code == 200
    assert asc.json()["items"][0]["id"] == id2

    search = client.get("/api/journeys/definitions", params={"search": "alpha"})
    assert search.status_code == 200
    assert search.json()["total"] == 1
    assert search.json()["items"][0]["id"] == id1


def test_journey_definition_validation_and_permissions(client: TestClient):

    no_perm = client.post(
        "/api/journeys/definitions",
        headers={"X-User-Role": "viewer", "X-User-Id": "qa-viewer"},
        json={"name": "Blocked", "conversion_kpi_id": "purchase", "lookback_window_days": 30, "mode_default": "conversion_only"},
    )
    assert no_perm.status_code == 403
    assert isinstance(no_perm.json().get("detail"), dict)
    assert no_perm.json()["detail"]["code"] == "permission_denied"
    assert no_perm.json()["detail"]["permission"] == "journeys.manage"

    blank_name = client.post(
        "/api/journeys/definitions",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={"name": "   ", "conversion_kpi_id": "purchase", "lookback_window_days": 30, "mode_default": "conversion_only"},
    )
    assert blank_name.status_code == 400
    assert "name is required" in blank_name.json()["detail"]

    invalid_kpi = client.post(
        "/api/journeys/definitions",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={"name": "Invalid KPI", "conversion_kpi_id": "not_a_kpi", "lookback_window_days": 30, "mode_default": "conversion_only"},
    )
    assert invalid_kpi.status_code == 400
    assert "conversion_kpi_id" in invalid_kpi.json()["detail"]

    invalid_lookback = client.post(
        "/api/journeys/definitions",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={"name": "Invalid lookback", "conversion_kpi_id": "purchase", "lookback_window_days": 366, "mode_default": "conversion_only"},
    )
    assert invalid_lookback.status_code == 422

    created = client.post(
        "/api/journeys/definitions",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={"name": "For update/delete", "conversion_kpi_id": "purchase", "lookback_window_days": 30, "mode_default": "conversion_only"},
    )
    assert created.status_code == 200
    definition_id = created.json()["id"]

    upd_no_perm = client.put(
        f"/api/journeys/definitions/{definition_id}",
        headers={"X-User-Role": "viewer", "X-User-Id": "qa-viewer"},
        json={"name": "No update", "conversion_kpi_id": "purchase", "lookback_window_days": 30, "mode_default": "conversion_only"},
    )
    assert upd_no_perm.status_code == 403

    del_no_perm = client.delete(
        f"/api/journeys/definitions/{definition_id}",
        headers={"X-User-Role": "viewer", "X-User-Id": "qa-viewer"},
    )
    assert del_no_perm.status_code == 403


def test_journey_definitions_list_accepts_aliases_and_clamps_page_size(client: TestClient):
    headers = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}
    for idx in range(3):
        created = client.post(
            "/api/journeys/definitions",
            headers=headers,
            json={
                "name": f"Journeys {idx}",
                "conversion_kpi_id": "purchase",
                "lookback_window_days": 30,
                "mode_default": "conversion_only",
            },
        )
        assert created.status_code == 200

    over_limit = client.get("/api/journeys/definitions", params={"per_page": 200, "sort": "desc"})
    assert over_limit.status_code == 200
    assert over_limit.json()["per_page"] == 100
    assert len(over_limit.json()["items"]) == 3

    page_size_alias = client.get("/api/journeys/definitions", params={"page_size": 2})
    assert page_size_alias.status_code == 200
    assert page_size_alias.json()["per_page"] == 2
    assert len(page_size_alias.json()["items"]) == 2

    limit_alias = client.get("/api/journeys/definitions", params={"limit": 1})
    assert limit_alias.status_code == 200
    assert limit_alias.json()["per_page"] == 1
    assert len(limit_alias.json()["items"]) == 1

    order_alias = client.get("/api/journeys/definitions", params={"order": "asc"})
    assert order_alias.status_code == 200
    assert order_alias.json()["per_page"] == 20
