from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
import app.main as main_module
from app.models_config_dq import ExperimentAssignment, ExperimentOutcome, JourneyPathDaily
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
            JourneyPathDaily(
                journey_definition_id=definition_id,
                date=date(2026, 3, 1),
                path_hash="recovery-path",
                path_steps=["Paid Social", "Product View", "Checkout", "Purchase"],
                path_length=4,
                count_journeys=90,
                count_conversions=22,
                gross_revenue_total=2200.0,
                net_revenue_total=2200.0,
                p50_time_to_convert_sec=4800,
                avg_time_to_convert_sec=5400,
                channel_group="meta_ads",
                campaign_id="meta-retargeting",
                device="mobile",
                country="us",
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
    assert insights["summary"]["paths_considered"] == 4
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

    simulate_policy = client.post(
        f"/api/journeys/hypotheses/{created['id']}/simulate",
        headers=viewer_headers,
        json={"proposed_step": "Checkout"},
    )
    assert simulate_policy.status_code == 200
    simulation = simulate_policy.json()
    assert simulation["previewAvailable"] is True
    assert simulation["prefix"]["label"] == "Paid Social > Product View"
    assert simulation["selected_policy"]["step"] == "Checkout"
    assert simulation["selected_policy"]["uplift_abs"] > 0
    assert any(candidate["step"] == "Exit" for candidate in simulation["top_candidates"])
    assert any(candidate["step"] == "Checkout" for candidate in simulation["top_candidates"])

    create_experiment = client.post(
        f"/api/journeys/hypotheses/{created['id']}/create-experiment",
        headers=viewer_headers,
        json={
            "start_at": "2026-04-05T00:00:00Z",
            "end_at": "2026-04-19T00:00:00Z",
            "notes": "Launch as a two-week holdout.",
            "proposed_step": "Checkout",
            "guardrails": {"min_runtime_days": 7},
        },
    )
    assert create_experiment.status_code == 200
    experiment_payload = create_experiment.json()
    assert experiment_payload["experiment"]["source_type"] == "journey_hypothesis"
    assert experiment_payload["experiment"]["source_id"] == created["id"]
    assert experiment_payload["experiment"]["source_name"] == updated["title"]
    assert experiment_payload["experiment"]["source_journey_definition_id"] == definition_id
    assert experiment_payload["hypothesis"]["linked_experiment_id"] == experiment_payload["experiment"]["id"]
    assert experiment_payload["hypothesis"]["status"] == "in_experiment"
    assert experiment_payload["hypothesis"]["proposed_action"]["step"] == "Checkout"

    list_experiments = client.get("/api/experiments", headers=viewer_headers)
    assert list_experiments.status_code == 200
    listed = list_experiments.json()
    assert len(listed) == 1
    assert listed[0]["source_type"] == "journey_hypothesis"
    assert listed[0]["source_name"] == updated["title"]
    assert listed[0]["source_journey_definition_id"] == definition_id

    filtered_experiments = client.get(
        "/api/experiments",
        params={"source_type": "journey_hypothesis", "source_id": [created["id"]]},
        headers=viewer_headers,
    )
    assert filtered_experiments.status_code == 200
    filtered = filtered_experiments.json()
    assert len(filtered) == 1
    assert filtered[0]["source_id"] == created["id"]
    assert filtered[0]["source_journey_definition_id"] == definition_id

    filtered_empty = client.get(
        "/api/experiments",
        params={"source_type": "journey_hypothesis", "source_id": ["missing-hypothesis"]},
        headers=viewer_headers,
    )
    assert filtered_empty.status_code == 200
    assert filtered_empty.json() == []

    experiment_detail = client.get(f"/api/experiments/{experiment_payload['experiment']['id']}", headers=viewer_headers)
    assert experiment_detail.status_code == 200
    detail = experiment_detail.json()
    assert detail["experiment_type"] == "holdout"
    assert detail["source_type"] == "journey_hypothesis"
    assert detail["source_journey_definition_id"] == definition_id
    assert detail["segment"]["channel_group"] == created["segment"]["channel_group"]
    assert detail["policy"]["proposed_action"]["type"] == created["proposed_action"]["type"]
    assert detail["policy"]["proposed_action"]["step"] == "Checkout"
    assert detail["guardrails"]["sample_size_target"] == created["sample_size_target"]

    exp_id = experiment_payload["experiment"]["id"]
    session = SessionLocal()
    try:
        assignments = [
            ExperimentAssignment(
                experiment_id=exp_id,
                profile_id=f"treatment-{idx}",
                group="treatment",
                assigned_at=datetime.utcnow(),
            )
            for idx in range(40)
        ] + [
            ExperimentAssignment(
                experiment_id=exp_id,
                profile_id=f"control-{idx}",
                group="control",
                assigned_at=datetime.utcnow(),
            )
            for idx in range(40)
        ]
        outcomes = [
            ExperimentOutcome(
                experiment_id=exp_id,
                profile_id=f"treatment-{idx}",
                conversion_ts=datetime(2026, 4, 10, 12, 0, 0),
                value=120.0,
            )
            for idx in range(20)
        ] + [
            ExperimentOutcome(
                experiment_id=exp_id,
                profile_id=f"control-{idx}",
                conversion_ts=datetime(2026, 4, 10, 12, 0, 0),
                value=80.0,
            )
            for idx in range(8)
        ]
        session.add_all(assignments + outcomes)
        session.commit()
    finally:
        session.close()

    complete_experiment = client.post(
        f"/api/experiments/{exp_id}/status",
        headers=viewer_headers,
        json={"status": "completed"},
    )
    assert complete_experiment.status_code == 200
    assert complete_experiment.json()["status"] == "completed"

    refreshed_hypotheses = client.get(
        "/api/journeys/hypotheses",
        params={"journey_definition_id": definition_id},
        headers=viewer_headers,
    )
    assert refreshed_hypotheses.status_code == 200
    refreshed = refreshed_hypotheses.json()["items"][0]
    assert refreshed["status"] == "validated"
    assert refreshed["result"]["verdict"] == "validated"
    assert refreshed["result"]["experiment_status"] == "completed"
    assert refreshed["result"]["treatment"]["n"] == 40
    assert refreshed["result"]["control"]["n"] == 40
    assert refreshed["result"]["uplift_abs"] > 0
    assert "Treatment beat control" in refreshed["result"]["summary"]

    policies = client.get(
        f"/api/journeys/{definition_id}/policies",
        headers=viewer_headers,
    )
    assert policies.status_code == 200
    policy_payload = policies.json()
    assert policy_payload["summary"]["validated"] >= 1
    assert policy_payload["summary"]["ready_to_promote"] >= 1
    assert policy_payload["items"][0]["recommendation"] == "promote"
    assert policy_payload["items"][0]["hypothesis_id"] == created["id"]

    promote_policy = client.post(
        f"/api/journeys/hypotheses/{created['id']}/policy-promotion",
        headers=viewer_headers,
        json={"active": True, "notes": "Promote for the mobile retargeting journey."},
    )
    assert promote_policy.status_code == 200
    promoted = promote_policy.json()
    assert promoted["hypothesis"]["result"]["policy_promotion"]["active"] is True
    assert promoted["policy_candidate"]["recommendation"] == "promoted"

    promoted_policies = client.get(
        f"/api/journeys/{definition_id}/policies",
        headers=viewer_headers,
    )
    assert promoted_policies.status_code == 200
    promoted_policy = promoted_policies.json()["items"][0]
    assert promoted_policy["promotion"]["active"] is True
    assert promoted_policy["recommendation"] == "promoted"
