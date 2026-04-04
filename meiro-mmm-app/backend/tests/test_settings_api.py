from datetime import datetime, timezone

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
import app.main as main_module
from app.models_config_dq import JourneyDefinition, JourneyHypothesis
from app.utils.kpi_config import default_kpi_config


@pytest.fixture
def client():
    original_kpi_config = main_module.KPI_CONFIG
    original_settings = main_module.SETTINGS.model_copy(deep=True)
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
    main_module.SETTINGS = original_settings
    engine.dispose()


@pytest.fixture
def client_and_session():
    original_kpi_config = main_module.KPI_CONFIG
    original_settings = main_module.SETTINGS.model_copy(deep=True)
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
        yield test_client, SessionLocal
    app.dependency_overrides.clear()
    main_module.KPI_CONFIG = original_kpi_config
    main_module.SETTINGS = original_settings
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


def test_sync_nba_promoted_policies_from_journey_lab(client_and_session):
    client, SessionLocal = client_and_session
    editor_headers = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}

    session = SessionLocal()
    try:
        journey = JourneyDefinition(
            id="jd-settings-sync",
            name="Journey Settings Sync",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="qa-editor",
            updated_by="qa-editor",
            is_archived=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        hypothesis = JourneyHypothesis(
            id="hyp-settings-sync",
            workspace_id="default",
            journey_definition_id="jd-settings-sync",
            owner_user_id="qa-editor",
            title="Promote checkout recovery",
            target_kpi="purchase",
            hypothesis_text="If we push checkout earlier, conversion should improve.",
            trigger_json={"steps": ["Paid Social", "Product View", "Exit"]},
            segment_json={"channel_group": "meta_ads", "device": "mobile"},
            current_action_json={"type": "exit"},
            proposed_action_json={"type": "nba_intervention", "step": "Checkout"},
            support_count=180,
            baseline_rate=0.05,
            sample_size_target=200,
            status="validated",
            linked_experiment_id=42,
            result_json={
                "learning_stage": "validated",
                "policy_promotion": {
                    "active": True,
                    "promoted_at": "2026-04-04T08:00:00Z",
                    "promoted_by": "qa-editor",
                    "notes": "Validated in experiment.",
                    "source": "validated_experiment",
                },
            },
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add_all([journey, hypothesis])
        session.commit()
    finally:
        session.close()

    resp = client.post("/api/settings/nba/promoted-policies/sync", headers=editor_headers)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["synced"] == 1
    assert payload["items"][0]["hypothesis_id"] == "hyp-settings-sync"
    assert payload["items"][0]["prefix"] == "Paid Social > Product View"
    assert payload["items"][0]["step"] == "Checkout"

    settings_resp = client.get("/api/settings", headers=editor_headers)
    assert settings_resp.status_code == 200
    settings_payload = settings_resp.json()
    assert settings_payload["nba"]["promoted_journey_policies"][0]["hypothesis_id"] == "hyp-settings-sync"
