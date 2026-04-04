from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
import app.main as main_module
from app.models_config_dq import JourneyPathDaily
from app.utils.kpi_config import default_kpi_config


@pytest.fixture
def client_and_session():
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
        yield test_client, SessionLocal
    app.dependency_overrides.clear()
    main_module.KPI_CONFIG = original_kpi_config
    engine.dispose()


def test_journey_insights_and_hypothesis_crud_flow(client_and_session):
    client, SessionLocal = client_and_session
    editor_headers = {"X-User-Role": "editor", "X-User-Id": "qa-editor"}
    viewer_headers = {"X-User-Role": "viewer", "X-User-Id": "qa-viewer"}

    create_definition = client.post(
        "/api/journeys/definitions",
        headers=editor_headers,
        json={
            "name": "Checkout journey",
            "description": "Journey with enough path coverage for insights",
            "conversion_kpi_id": "purchase",
            "lookback_window_days": 30,
            "mode_default": "conversion_only",
        },
    )
    assert create_definition.status_code == 200
    definition_id = create_definition.json()["id"]

    session = SessionLocal()
    try:
        rows = [
            JourneyPathDaily(
                journey_definition_id=definition_id,
                date=date(2026, 3, 1),
                path_hash="weak-path",
                path_steps=["Paid Social", "Product View", "Exit"],
                path_length=3,
                count_journeys=180,
                count_conversions=6,
                gross_revenue_total=600.0,
                net_revenue_total=600.0,
                p50_time_to_convert_sec=7200,
                avg_time_to_convert_sec=8400,
                channel_group="meta_ads",
                campaign_id="meta-retargeting",
                device="mobile",
                country="us",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
            JourneyPathDaily(
                journey_definition_id=definition_id,
                date=date(2026, 3, 1),
                path_hash="best-path",
                path_steps=["Email", "Product View", "Checkout", "Purchase"],
                path_length=4,
                count_journeys=140,
                count_conversions=28,
                gross_revenue_total=2800.0,
                net_revenue_total=2800.0,
                p50_time_to_convert_sec=5400,
                avg_time_to_convert_sec=6000,
                channel_group="email",
                campaign_id="email-winback",
                device="desktop",
                country="us",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
            JourneyPathDaily(
                journey_definition_id=definition_id,
                date=date(2026, 3, 1),
                path_hash="slow-path",
                path_steps=["Organic", "Blog", "Signup", "Purchase"],
                path_length=4,
                count_journeys=120,
                count_conversions=14,
                gross_revenue_total=1400.0,
                net_revenue_total=1400.0,
                p50_time_to_convert_sec=172800,
                avg_time_to_convert_sec=180000,
                channel_group="organic",
                campaign_id="content",
                device="desktop",
                country="gb",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
        ]
        session.add_all(rows)
        session.commit()
    finally:
        session.close()

    insights_resp = client.get(
        f"/api/journeys/{definition_id}/insights",
        params={"date_from": "2026-03-01", "date_to": "2026-03-31", "mode": "conversion_only"},
        headers=viewer_headers,
    )
    assert insights_resp.status_code == 200
    insights = insights_resp.json()
    assert insights["summary"]["paths_considered"] == 3
    assert len(insights["items"]) >= 2
    first_hypothesis = insights["items"][0]["suggested_hypothesis"]

    create_hypothesis = client.post(
        "/api/journeys/hypotheses",
        headers=viewer_headers,
        json={
            "journey_definition_id": definition_id,
            "title": first_hypothesis["title"],
            "target_kpi": "purchase",
            "hypothesis_text": first_hypothesis["hypothesis_text"],
            "trigger": first_hypothesis["trigger"],
            "segment": first_hypothesis["segment"],
            "current_action": first_hypothesis["current_action"],
            "proposed_action": first_hypothesis["proposed_action"],
            "support_count": first_hypothesis["support_count"],
            "baseline_rate": first_hypothesis["baseline_rate"],
            "sample_size_target": first_hypothesis["sample_size_target"],
            "status": "draft",
            "result": {},
        },
    )
    assert create_hypothesis.status_code == 200
    created = create_hypothesis.json()
    assert created["journey_definition_id"] == definition_id
    assert created["owner_user_id"] == "qa-viewer"
    assert created["status"] == "draft"

    list_hypotheses = client.get(
        "/api/journeys/hypotheses",
        params={"journey_definition_id": definition_id},
        headers=viewer_headers,
    )
    assert list_hypotheses.status_code == 200
    assert list_hypotheses.json()["total"] == 1

    update_hypothesis = client.put(
        f"/api/journeys/hypotheses/{created['id']}",
        headers=viewer_headers,
        json={
            "journey_definition_id": definition_id,
            "title": f"{created['title']} v2",
            "target_kpi": "purchase",
            "hypothesis_text": created["hypothesis_text"],
            "trigger": created["trigger"],
            "segment": created["segment"],
            "current_action": created["current_action"],
            "proposed_action": created["proposed_action"],
            "support_count": created["support_count"],
            "baseline_rate": created["baseline_rate"],
            "sample_size_target": created["sample_size_target"],
            "status": "ready_to_test",
            "result": {"note": "Needs sample sizing confirmation"},
        },
    )
    assert update_hypothesis.status_code == 200
    updated = update_hypothesis.json()
    assert updated["title"].endswith("v2")
    assert updated["status"] == "ready_to_test"
    assert updated["result"]["note"] == "Needs sample sizing confirmation"
