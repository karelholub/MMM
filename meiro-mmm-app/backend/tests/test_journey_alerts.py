from datetime import date, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models_config_dq import (
    FunnelDefinition,
    JourneyAlertEvent,
    JourneyDefinition,
    JourneyDefinitionInstanceFact,
    JourneyPathDaily,
    JourneyTransitionFact,
    JourneyTransitionDaily,
)
from app.services_journey_alerts import evaluate_alert_definitions


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
        yield test_client, SessionLocal
    app.dependency_overrides.clear()
    engine.dispose()


def _seed_paths(session_factory):
    db = session_factory()
    try:
        jd = JourneyDefinition(
            id="jd-alert",
            name="Alert Journey",
            description="",
            conversion_kpi_id="purchase",
            lookback_window_days=30,
            mode_default="conversion_only",
            created_by="seed",
            updated_by="seed",
            is_archived=False,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(jd)
        today = date.today()
        for d in range(1, 15):
            dt = today - timedelta(days=d)
            # Baseline ~20% CR and current ~10% CR.
            row = JourneyPathDaily(
                date=dt,
                journey_definition_id="jd-alert",
                path_hash="p-1",
                path_steps=["paid", "email"],
                path_length=2,
                count_journeys=100,
                count_conversions=10 if d <= 7 else 20,
                gross_conversions_total=float(10 if d <= 7 else 20),
                net_conversions_total=float(8 if d <= 7 else 18),
                gross_revenue_total=float(500 if d <= 7 else 1000),
                net_revenue_total=float(400 if d <= 7 else 900),
                view_through_conversions_total=0.0,
                click_through_conversions_total=float(10 if d <= 7 else 20),
                mixed_path_conversions_total=0.0,
                avg_time_to_convert_sec=120.0,
                p50_time_to_convert_sec=100.0,
                p90_time_to_convert_sec=240.0,
                channel_group="paid",
                campaign_id="cmp-1",
                device="mobile",
                country="US",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(row)
        db.commit()
    finally:
        db.close()


def _seed_funnel(session_factory):
    db = session_factory()
    try:
        jd = db.get(JourneyDefinition, "jd-alert")
        if not jd:
            jd = JourneyDefinition(
                id="jd-alert",
                name="Alert Journey",
                conversion_kpi_id="purchase",
                lookback_window_days=30,
                mode_default="conversion_only",
                created_by="seed",
                updated_by="seed",
                is_archived=False,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(jd)
        funnel = FunnelDefinition(
            id="f-alert",
            journey_definition_id="jd-alert",
            workspace_id="default",
            user_id="qa",
            name="Signup funnel",
            description="",
            steps_json=["Landing", "Signup", "Purchase"],
            counting_method="ordered",
            window_days=30,
            is_archived=False,
            created_by="qa",
            updated_by="qa",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(funnel)
        today = date.today()
        for d in range(1, 15):
            dt = today - timedelta(days=d)
            # Current 7d dropoff from Landing->Signup is worse than baseline.
            curr = d <= 7
            db.add(
                JourneyTransitionDaily(
                    date=dt,
                    journey_definition_id="jd-alert",
                    from_step="Landing",
                    to_step="Signup",
                    count_transitions=20 if curr else 60,
                    count_profiles=20 if curr else 60,
                    channel_group="paid",
                    campaign_id="cmp-1",
                    device="mobile",
                    country="US",
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
            )
            db.add(
                JourneyTransitionDaily(
                    date=dt,
                    journey_definition_id="jd-alert",
                    from_step="Landing",
                    to_step="Other",
                    count_transitions=80 if curr else 40,
                    count_profiles=80 if curr else 40,
                    channel_group="paid",
                    campaign_id="cmp-1",
                    device="mobile",
                    country="US",
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
            )
        db.commit()
    finally:
        db.close()


def _seed_funnel_transition_facts(session_factory):
    db = session_factory()
    try:
        jd = db.get(JourneyDefinition, "jd-alert")
        if not jd:
            jd = JourneyDefinition(
                id="jd-alert",
                name="Alert Journey",
                conversion_kpi_id="purchase",
                lookback_window_days=30,
                mode_default="conversion_only",
                created_by="seed",
                updated_by="seed",
                is_archived=False,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(jd)
        funnel = FunnelDefinition(
            id="f-alert-facts",
            journey_definition_id="jd-alert",
            workspace_id="default",
            user_id="qa",
            name="Signup funnel facts",
            description="",
            steps_json=["Landing", "Signup", "Purchase"],
            counting_method="ordered",
            window_days=30,
            is_archived=False,
            created_by="qa",
            updated_by="qa",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(funnel)
        today = date.today()
        for d in range(1, 15):
            day_value = today - timedelta(days=d)
            curr = d <= 7
            good_count = 1 if curr else 4
            bad_count = 4 if curr else 1
            ordinal = 0
            for idx in range(good_count):
                conversion_id = f"good-{d}-{idx}"
                db.add(
                    JourneyTransitionFact(
                        conversion_id=conversion_id,
                        profile_id=conversion_id,
                        conversion_key="purchase",
                        conversion_ts=datetime.combine(day_value, datetime.min.time()) + timedelta(hours=12, minutes=idx),
                        ordinal=ordinal,
                        from_step="Landing",
                        to_step="Signup",
                        from_step_ts=datetime.combine(day_value, datetime.min.time()) + timedelta(hours=10),
                        to_step_ts=datetime.combine(day_value, datetime.min.time()) + timedelta(hours=11),
                        delta_sec=3600.0,
                        channel_group="paid",
                        campaign_id="cmp-1",
                        device="mobile",
                        country="US",
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                    )
                )
            for idx in range(bad_count):
                conversion_id = f"bad-{d}-{idx}"
                db.add(
                    JourneyTransitionFact(
                        conversion_id=conversion_id,
                        profile_id=conversion_id,
                        conversion_key="purchase",
                        conversion_ts=datetime.combine(day_value, datetime.min.time()) + timedelta(hours=13, minutes=idx),
                        ordinal=0,
                        from_step="Landing",
                        to_step="Other",
                        from_step_ts=datetime.combine(day_value, datetime.min.time()) + timedelta(hours=10),
                        to_step_ts=datetime.combine(day_value, datetime.min.time()) + timedelta(hours=11, minutes=30),
                        delta_sec=5400.0,
                        channel_group="paid",
                        campaign_id="cmp-1",
                        device="mobile",
                        country="US",
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                    )
                )
        db.commit()
    finally:
        db.close()


def test_alert_permissions_and_preview(client):
    test_client, session_factory = client
    _seed_paths(session_factory)

    denied = test_client.post(
        "/api/alerts",
        headers={"X-User-Role": "viewer", "X-User-Id": "qa-viewer"},
        json={
            "name": "CR drop",
            "type": "path_cr_drop",
            "domain": "journeys",
            "scope": {"journey_definition_id": "jd-alert", "path_hash": "p-1"},
            "metric": "conversion_rate",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 30},
        },
    )
    assert denied.status_code == 403

    preview = test_client.post(
        "/api/alerts/preview",
        json={
            "type": "path_cr_drop",
            "scope": {"journey_definition_id": "jd-alert", "path_hash": "p-1"},
            "metric": "conversion_rate",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 30},
        },
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["current_value"] is not None
    assert body["baseline_value"] is not None
    assert body["delta_pct"] < 0


def test_alert_evaluator_threshold_and_dedupe(client):
    test_client, session_factory = client
    _seed_paths(session_factory)

    created = test_client.post(
        "/api/alerts",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Path conversion rate drop",
            "type": "path_cr_drop",
            "domain": "journeys",
            "scope": {"journey_definition_id": "jd-alert", "path_hash": "p-1"},
            "metric": "conversion_rate",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 20, "cooldown_days": 2},
            "schedule": {"cadence": "daily"},
        },
    )
    assert created.status_code == 200

    db = session_factory()
    try:
        out1 = evaluate_alert_definitions(db, domain="journeys")
        assert out1["evaluated"] >= 1
        assert out1["fired"] == 1
        out2 = evaluate_alert_definitions(db, domain="journeys")
        assert out2["fired"] == 0  # same-day dedupe
        assert db.query(JourneyAlertEvent).count() == 1
    finally:
        db.close()


def test_alert_preview_supports_aggregate_revenue_metric(client):
    test_client, session_factory = client
    _seed_paths(session_factory)

    preview = test_client.post(
        "/api/alerts/preview",
        json={
            "type": "path_volume_change",
            "scope": {"journey_definition_id": "jd-alert", "path_hash": "p-1"},
            "metric": "gross_revenue_total",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 20},
        },
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["current_value"] == 3000.0
    assert body["baseline_value"] == 6500.0
    assert round(body["delta_pct"], 4) == round(((3000.0 - 6500.0) / 6500.0) * 100.0, 4)


def test_alert_preview_falls_back_to_definition_instance_facts_when_daily_rows_absent(client):
    test_client, session_factory = client
    db = session_factory()
    try:
        db.add(
            JourneyDefinition(
                id="jd-alert-facts",
                name="Alert Facts Journey",
                description="",
                conversion_kpi_id="purchase",
                lookback_window_days=30,
                mode_default="conversion_only",
                created_by="seed",
                updated_by="seed",
                is_archived=False,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )
        today = date.today()
        for d in range(1, 15):
            dt = today - timedelta(days=d)
            revenue = 300.0 if d <= 7 else 650.0
            for idx in range(1 if d <= 7 else 2):
                db.add(
                    JourneyDefinitionInstanceFact(
                        date=dt,
                        journey_definition_id="jd-alert-facts",
                        conversion_id=f"conv-{d}-{idx}",
                        profile_id=f"p-{d}-{idx}",
                        conversion_key="purchase",
                        conversion_ts=datetime.combine(dt, datetime.min.time()) + timedelta(hours=12, minutes=idx),
                        path_hash="p-facts",
                        steps_json=["Paid Landing", "Email", "Purchase / Lead Won (conversion)"],
                        path_length=3,
                        channel_group="paid",
                        last_touch_channel="email",
                        campaign_id="cmp-1",
                        device="mobile",
                        country="US",
                        interaction_path_type="click_through",
                        time_to_convert_sec=100.0,
                        gross_conversions_total=1.0,
                        net_conversions_total=1.0,
                        gross_revenue_total=revenue,
                        net_revenue_total=revenue,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                    )
                )
        db.commit()
    finally:
        db.close()

    preview = test_client.post(
        "/api/alerts/preview",
        json={
            "type": "path_volume_change",
            "scope": {"journey_definition_id": "jd-alert-facts", "path_hash": "p-facts"},
            "metric": "gross_revenue_total",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 20},
        },
    )
    assert preview.status_code == 200
    body = preview.json()
    assert body["current_value"] == 1800.0
    assert body["baseline_value"] == 8100.0


def test_funnel_dropoff_alert_type(client):
    test_client, session_factory = client
    _seed_funnel(session_factory)

    created = test_client.post(
        "/api/alerts",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Signup dropoff spike",
            "type": "funnel_dropoff_spike",
            "domain": "funnels",
            "scope": {"journey_definition_id": "jd-alert", "funnel_id": "f-alert", "step_index": 0},
            "metric": "dropoff_rate",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 20},
            "schedule": {"cadence": "daily"},
        },
    )
    assert created.status_code == 200

    events = test_client.get("/api/alerts/events", params={"domain": "funnels"})
    assert events.status_code == 200
    assert events.json()["total"] == 0

    db = session_factory()
    try:
        out = evaluate_alert_definitions(db, domain="funnels")
        assert out["fired"] == 1
    finally:
        db.close()


def test_funnel_dropoff_alert_falls_back_to_transition_facts(client):
    test_client, session_factory = client
    _seed_funnel_transition_facts(session_factory)

    created = test_client.post(
        "/api/alerts",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Signup dropoff spike from facts",
            "type": "funnel_dropoff_spike",
            "domain": "funnels",
            "scope": {"journey_definition_id": "jd-alert", "funnel_id": "f-alert-facts", "step_index": 0},
            "metric": "dropoff_rate",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 20},
            "schedule": {"cadence": "daily"},
        },
    )
    assert created.status_code == 200

    db = session_factory()
    try:
        out = evaluate_alert_definitions(db, domain="funnels")
        assert out["fired"] == 1
    finally:
        db.close()


def test_alert_list_and_events_accept_aliases_and_clamp_page_size(client):
    test_client, session_factory = client
    _seed_paths(session_factory)

    created = test_client.post(
        "/api/alerts",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Path volume change",
            "type": "path_volume_change",
            "domain": "journeys",
            "scope": {"journey_definition_id": "jd-alert", "path_hash": "p-1"},
            "metric": "count_journeys",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 20, "cooldown_days": 2},
            "schedule": {"cadence": "daily"},
        },
    )
    assert created.status_code == 200

    defs = test_client.get("/api/alerts", params={"domain": "journeys", "per_page": 999})
    assert defs.status_code == 200
    assert defs.json()["per_page"] == 100
    assert defs.json()["total"] >= 1

    defs_alias = test_client.get("/api/alerts", params={"domain": "journeys", "page_size": 1})
    assert defs_alias.status_code == 200
    assert defs_alias.json()["per_page"] == 1

    db = session_factory()
    try:
        out = evaluate_alert_definitions(db, domain="journeys")
        assert out["evaluated"] >= 1
    finally:
        db.close()

    events = test_client.get("/api/alerts/events", params={"domain": "journeys", "per_page": 500})
    assert events.status_code == 200
    assert events.json()["per_page"] == 200
    assert events.json()["total"] >= 0

    events_alias = test_client.get("/api/alerts/events", params={"domain": "journeys", "limit": 1})
    assert events_alias.status_code == 200
    assert events_alias.json()["per_page"] == 1

    events2 = test_client.get("/api/alerts/events", params={"domain": "funnels"})
    assert events2.status_code == 200
    assert events2.json()["total"] == 0


def test_alert_create_rejects_invalid_scope_and_metric(client):
    test_client, session_factory = client
    _seed_paths(session_factory)

    bad_metric = test_client.post(
        "/api/alerts",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Bad metric",
            "type": "path_cr_drop",
            "domain": "journeys",
            "scope": {"journey_definition_id": "jd-alert", "path_hash": "p-1"},
            "metric": "count_journeys",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 20},
        },
    )
    assert bad_metric.status_code == 400

    missing_scope = test_client.post(
        "/api/alerts",
        headers={"X-User-Role": "editor", "X-User-Id": "qa-editor"},
        json={
            "name": "Missing scope",
            "type": "path_volume_change",
            "domain": "journeys",
            "scope": {},
            "metric": "count_journeys",
            "condition": {"comparison_mode": "previous_period", "window_days": 7, "threshold_pct": 20},
        },
    )
    assert missing_scope.status_code == 400
