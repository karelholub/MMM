from fastapi.testclient import TestClient
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


def test_journey_saved_views_crud_flow(client: TestClient):
    editor_headers = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}
    viewer_headers = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}

    create_definition = client.post(
        "/api/journeys/definitions",
        headers=editor_headers,
        json={
            "name": "Checkout journey",
            "description": "Shared definition for saved views",
            "conversion_kpi_id": "purchase",
            "lookback_window_days": 30,
            "mode_default": "conversion_only",
        },
    )
    assert create_definition.status_code == 200
    definition_id = create_definition.json()["id"]

    create_view = client.post(
        "/api/journeys/views",
        headers=viewer_headers,
        json={
            "name": "Weekly checkout investigation",
            "journey_definition_id": definition_id,
            "state": {
                "selectedJourneyId": definition_id,
                "activeTab": "examples",
                "filters": {
                    "dateFrom": "2026-03-01",
                    "dateTo": "2026-03-25",
                    "channel": "paid_search",
                    "campaign": "all",
                    "device": "desktop",
                    "geo": "us",
                    "segment": "all",
                },
                "pathSortBy": "journeys",
                "pathSortDir": "desc",
                "pathsLimit": 50,
                "examplesPathHash": "abc123",
                "examplesStepFilter": "checkout",
            },
        },
    )
    assert create_view.status_code == 200
    created = create_view.json()
    assert created["name"] == "Weekly checkout investigation"
    assert created["journey_definition_id"] == definition_id
    assert created["state"]["activeTab"] == "examples"
    view_id = created["id"]

    list_views = client.get("/api/journeys/views", headers=viewer_headers)
    assert list_views.status_code == 200
    payload = list_views.json()
    assert payload["total"] == 1
    assert payload["items"][0]["id"] == view_id

    update_view = client.put(
        f"/api/journeys/views/{view_id}",
        headers=viewer_headers,
        json={
            "name": "Weekly checkout investigation v2",
            "journey_definition_id": definition_id,
            "state": {
                "selectedJourneyId": definition_id,
                "activeTab": "flow",
                "filters": {
                    "dateFrom": "2026-03-01",
                    "dateTo": "2026-03-25",
                    "channel": "all",
                    "campaign": "all",
                    "device": "all",
                    "geo": "all",
                    "segment": "all",
                },
                "pathSortBy": "conversion_rate",
                "pathSortDir": "asc",
                "pathsLimit": 25,
                "examplesPathHash": "",
                "examplesStepFilter": "",
            },
        },
    )
    assert update_view.status_code == 200
    updated = update_view.json()
    assert updated["name"] == "Weekly checkout investigation v2"
    assert updated["state"]["activeTab"] == "flow"
    assert updated["state"]["pathSortBy"] == "conversion_rate"

    other_user_list = client.get("/api/journeys/views", headers=editor_headers)
    assert other_user_list.status_code == 200
    assert other_user_list.json()["total"] == 0

    delete_view = client.delete(f"/api/journeys/views/{view_id}", headers=viewer_headers)
    assert delete_view.status_code == 200
    assert delete_view.json()["status"] == "deleted"

    list_after_delete = client.get("/api/journeys/views", headers=viewer_headers)
    assert list_after_delete.status_code == 200
    assert list_after_delete.json()["total"] == 0
