from fastapi.testclient import TestClient
from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
import app.main as main_module
from app.utils.kpi_config import default_kpi_config


@pytest.fixture
def client():
    original_kpi_config = main_module.KPI_CONFIG
    main_module.KPI_CONFIG = default_kpi_config()

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
    main_module.KPI_CONFIG = original_kpi_config
    engine.dispose()


def test_journey_definitions_crud_and_archive_flow(client: TestClient):
    view_headers = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}
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

    list_resp = client.get("/api/journeys/definitions", headers=view_headers)
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

    list_active = client.get("/api/journeys/definitions", headers=view_headers)
    assert list_active.status_code == 200
    assert list_active.json()["total"] == 0

    list_all = client.get("/api/journeys/definitions", params={"include_archived": "true"}, headers=view_headers)
    assert list_all.status_code == 200
    assert list_all.json()["total"] == 1
    assert list_all.json()["items"][0]["is_archived"] is True

    restore_resp = client.post(
        f"/api/journeys/definitions/{definition_id}/restore",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
    )
    assert restore_resp.status_code == 200
    assert restore_resp.json()["is_archived"] is False

    duplicate_resp = client.post(
        f"/api/journeys/definitions/{definition_id}/duplicate",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={"name": "Lifecycle journey copy"},
    )
    assert duplicate_resp.status_code == 200
    assert duplicate_resp.json()["name"] == "Lifecycle journey copy"
    assert duplicate_resp.json()["id"] != definition_id


def test_journey_definitions_list_search_and_sort(client: TestClient):
    headers = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}
    view_headers = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}

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

    desc = client.get("/api/journeys/definitions", params={"sort": "desc"}, headers=view_headers)
    assert desc.status_code == 200
    assert desc.json()["items"][0]["id"] == id1

    asc = client.get("/api/journeys/definitions", params={"sort": "asc"}, headers=view_headers)
    assert asc.status_code == 200
    assert asc.json()["items"][0]["id"] == id2

    search = client.get("/api/journeys/definitions", params={"search": "alpha"}, headers=view_headers)
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
    view_headers = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}
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

    over_limit = client.get("/api/journeys/definitions", params={"per_page": 200, "sort": "desc"}, headers=view_headers)
    assert over_limit.status_code == 200
    assert over_limit.json()["per_page"] == 100
    assert len(over_limit.json()["items"]) == 3

    page_size_alias = client.get("/api/journeys/definitions", params={"page_size": 2}, headers=view_headers)
    assert page_size_alias.status_code == 200
    assert page_size_alias.json()["per_page"] == 2
    assert len(page_size_alias.json()["items"]) == 2

    limit_alias = client.get("/api/journeys/definitions", params={"limit": 1}, headers=view_headers)
    assert limit_alias.status_code == 200
    assert limit_alias.json()["per_page"] == 1
    assert len(limit_alias.json()["items"]) == 1

    order_alias = client.get("/api/journeys/definitions", params={"order": "asc"}, headers=view_headers)
    assert order_alias.status_code == 200
    assert order_alias.json()["per_page"] == 20


def test_journey_definition_crud_triggers_rebuild_and_purge_hooks(client: TestClient, monkeypatch):
    rebuild_calls = []
    purge_calls = []

    monkeypatch.setattr(
        main_module,
        "rebuild_journey_definition_outputs",
        lambda db, definition_id, reprocess_days=None: rebuild_calls.append((definition_id, reprocess_days)) or {"definition_id": definition_id},
    )
    monkeypatch.setattr(
        main_module,
        "purge_journey_definition_outputs",
        lambda db, definition_id: purge_calls.append(definition_id) or {"definition_id": definition_id},
    )

    headers = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}
    create_resp = client.post(
        "/api/journeys/definitions",
        headers=headers,
        json={"name": "Hooked Journey", "conversion_kpi_id": "purchase", "lookback_window_days": 30, "mode_default": "conversion_only"},
    )
    assert create_resp.status_code == 200
    definition_id = create_resp.json()["id"]
    assert rebuild_calls == [(definition_id, None)]

    update_resp = client.put(
        f"/api/journeys/definitions/{definition_id}",
        headers=headers,
        json={"name": "Hooked Journey v2", "conversion_kpi_id": "lead", "lookback_window_days": 21, "mode_default": "all_journeys"},
    )
    assert update_resp.status_code == 200
    assert rebuild_calls == [(definition_id, None), (definition_id, None)]

    archive_resp = client.delete(
        f"/api/journeys/definitions/{definition_id}",
        headers=headers,
    )
    assert archive_resp.status_code == 200
    assert purge_calls == []


def test_journey_definition_lifecycle_reports_dependencies_and_actions(client: TestClient):
    editor_headers = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}
    view_headers = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}

    create_resp = client.post(
        "/api/journeys/definitions",
        headers=editor_headers,
        json={
            "name": "Lifecycle reporting",
            "description": "Definition with dependencies",
            "conversion_kpi_id": "purchase",
            "lookback_window_days": 30,
            "mode_default": "conversion_only",
        },
    )
    assert create_resp.status_code == 200
    definition_id = create_resp.json()["id"]

    view_resp = client.post(
        "/api/journeys/views",
        headers=view_headers,
        json={"name": "Saved view", "journey_definition_id": definition_id, "state": {"tab": "paths"}},
    )
    assert view_resp.status_code == 200

    funnel_resp = client.post(
        "/api/funnels",
        headers=editor_headers,
        json={
            "journey_definition_id": definition_id,
            "workspace_id": "default",
            "name": "Lifecycle funnel",
            "description": "Dependency funnel",
            "steps": ["Paid Landing", "Purchase / Lead Won"],
            "counting_method": "ordered",
            "window_days": 30,
        },
    )
    assert funnel_resp.status_code == 200

    hypothesis_resp = client.post(
        "/api/journeys/hypotheses",
        headers=view_headers,
        json={
            "journey_definition_id": definition_id,
            "title": "Lifecycle hypothesis",
            "target_kpi": "purchase",
            "hypothesis_text": "Testing next best action on this path.",
            "trigger": {"path_prefix": ["Paid Landing"]},
            "segment": {"channel_group": "paid"},
            "current_action": {"step": "wait"},
            "proposed_action": {"step": "email_followup"},
            "support_count": 12,
            "baseline_rate": 0.15,
            "sample_size_target": 100,
            "status": "draft",
            "result": {},
        },
    )
    assert hypothesis_resp.status_code == 200
    hypothesis_id = hypothesis_resp.json()["id"]

    experiment_resp = client.post(
        f"/api/journeys/hypotheses/{hypothesis_id}/create-experiment",
        headers=view_headers,
        json={
            "start_at": datetime(2026, 4, 1, tzinfo=timezone.utc).isoformat(),
            "end_at": datetime(2026, 4, 15, tzinfo=timezone.utc).isoformat(),
            "name": "Lifecycle hypothesis experiment",
            "channel": "journey",
            "notes": "Lifecycle dependency test",
            "experiment_type": "holdout",
        },
    )
    assert experiment_resp.status_code == 200

    lifecycle_resp = client.get(
        f"/api/journeys/definitions/{definition_id}/lifecycle",
        headers=view_headers,
    )
    assert lifecycle_resp.status_code == 200
    lifecycle = lifecycle_resp.json()
    assert lifecycle["definition"]["id"] == definition_id
    assert lifecycle["definition"]["lifecycle_status"] == "stale"
    assert lifecycle["rebuild_state"]["stale_reason"] == "no_outputs_built"
    assert lifecycle["dependency_counts"]["saved_views"] == 1
    assert lifecycle["dependency_counts"]["funnels"] == 1
    assert lifecycle["dependency_counts"]["hypotheses"] == 1
    assert lifecycle["dependency_counts"]["experiments"] == 1
    assert lifecycle["allowed_actions"]["can_archive"] is True
    assert lifecycle["allowed_actions"]["can_restore"] is False
    assert lifecycle["allowed_actions"]["can_duplicate"] is True
    assert lifecycle["warnings"]


def test_journey_definition_rebuild_endpoint_invokes_definition_job(client: TestClient, monkeypatch):
    calls = []
    monkeypatch.setattr(
        main_module,
        "rebuild_journey_definition_outputs",
        lambda db, definition_id, reprocess_days=None: calls.append((definition_id, reprocess_days)) or {
            "definition_id": definition_id,
            "days_processed": 2,
        },
    )

    headers = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}
    create_resp = client.post(
        "/api/journeys/definitions",
        headers=headers,
        json={"name": "Manual rebuild", "conversion_kpi_id": "purchase", "lookback_window_days": 30, "mode_default": "conversion_only"},
    )
    assert create_resp.status_code == 200
    definition_id = create_resp.json()["id"]
    calls.clear()

    rebuild_resp = client.post(
        f"/api/journeys/definitions/{definition_id}/rebuild",
        headers=headers,
        params={"reprocess_days": 7},
    )
    assert rebuild_resp.status_code == 200
    assert calls == [(definition_id, 7)]
    assert rebuild_resp.json()["metrics"]["days_processed"] == 2
