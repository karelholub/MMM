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


def test_update_taxonomy_invokes_shared_rebuild_job(client: TestClient, monkeypatch):
    calls = []

    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_outputs_for_taxonomy_change",
        lambda db, taxonomy=None, reprocess_days=None: calls.append((taxonomy, reprocess_days)) or {"taxonomy": {}, "journey_outputs": {}},
    )

    resp = client.post(
        "/api/taxonomy",
        json={
            "channel_rules": [
                {
                    "name": "Paid search",
                    "channel": "paid_search",
                    "priority": 10,
                    "enabled": True,
                    "source": {"operator": "equals", "value": "google"},
                    "medium": {"operator": "equals", "value": "cpc"},
                    "campaign": {"operator": "any", "value": ""},
                }
            ],
            "source_aliases": {},
            "medium_aliases": {},
        },
    )

    assert resp.status_code == 200
    assert len(calls) == 1
    assert calls[0][0] is not None


def test_update_kpis_rejects_definitions_referenced_by_active_journeys(client: TestClient):
    create_resp = client.post(
        "/api/journeys/definitions",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Lead journey",
            "description": "References lead KPI",
            "conversion_kpi_id": "lead",
            "lookback_window_days": 30,
            "mode_default": "conversion_only",
        },
    )
    assert create_resp.status_code == 200

    resp = client.post(
        "/api/kpis",
        json={
            "definitions": [
                {"id": "purchase", "label": "Purchase", "type": "conversion", "event_name": "purchase"},
            ],
            "primary_kpi_id": "purchase",
        },
    )

    assert resp.status_code == 400
    assert "Lead journey (lead)" in resp.json()["detail"]


def test_update_kpis_invokes_shared_rebuild_job(client: TestClient, monkeypatch):
    calls = []

    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_outputs_for_kpi_config_change",
        lambda db, previous_cfg, current_cfg, reprocess_days=None: calls.append((previous_cfg.primary_kpi_id, current_cfg.primary_kpi_id, reprocess_days)) or {"rebuild": {}},
    )

    resp = client.post(
        "/api/kpis",
        json={
            "definitions": [
                {"id": "purchase", "label": "Purchase", "type": "conversion", "event_name": "purchase"},
                {"id": "lead", "label": "Lead", "type": "conversion", "event_name": "lead_submit"},
            ],
            "primary_kpi_id": "lead",
        },
    )

    assert resp.status_code == 200
    assert resp.json()["primary_kpi_id"] == "lead"
    assert calls == [("purchase", "lead", None)]


def test_taxonomy_rebuild_endpoint_invokes_shared_job(client: TestClient, monkeypatch):
    calls = []

    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_outputs_for_taxonomy_change",
        lambda db, taxonomy=None, reprocess_days=None: calls.append((taxonomy, reprocess_days)) or {
            "taxonomy": {"source": "db_touchpoint_facts", "backfill": {"buckets_processed": 2}, "snapshots": [1, 2, 3]},
            "journey_outputs": {"definitions_rebuilt": 4},
        },
    )

    resp = client.post(
        "/api/taxonomy/rebuild",
        params={"reprocess_days": 7},
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
    )

    assert resp.status_code == 200
    assert calls == [(None, 7)]
    assert resp.json()["taxonomy"]["computed"] == 3
    assert resp.json()["journey_outputs"]["definitions_rebuilt"] == 4


def test_kpi_rebuild_endpoint_invokes_definition_job(client: TestClient, monkeypatch):
    calls = []

    monkeypatch.setattr(
        "app.services_rebuild_jobs.rebuild_multiple_journey_definition_outputs",
        lambda db, definition_ids=None, reprocess_days=None: calls.append((definition_ids, reprocess_days)) or {
            "definitions_rebuilt": 2,
            "effective_reprocess_days": reprocess_days,
        },
    )

    resp = client.post(
        "/api/kpis/rebuild-dependent-journeys",
        params={"reprocess_days": 5},
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
    )

    assert resp.status_code == 200
    assert calls == [(None, 5)]
    assert resp.json()["definitions_rebuilt"] == 2
