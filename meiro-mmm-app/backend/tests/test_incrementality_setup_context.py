from datetime import datetime

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
import app.main as main_module
from app.models_config_dq import ConversionPath
from app.services_journey_cache import invalidate_journey_cache
from app.utils.kpi_config import default_kpi_config


def test_incrementality_setup_context_uses_settings_and_observed_journeys():
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

    session = SessionLocal()
    try:
        invalidate_journey_cache()
        session.add_all(
            [
                ConversionPath(
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 3, 2, 12, 0),
                    path_json={
                        "customer_id": "p-1",
                        "touchpoints": [
                            {"channel": "email", "timestamp": "2026-03-01T09:00:00Z"},
                            {"channel": "direct", "timestamp": "2026-03-02T10:00:00Z"},
                        ],
                        "converted": True,
                        "conversion_value": 120.0,
                        "kpi_type": "purchase",
                        "conversions": [{"name": "purchase", "value": 120.0}],
                    },
                    path_hash="hash-1",
                    length=2,
                    first_touch_ts=datetime(2026, 3, 1, 9, 0),
                    last_touch_ts=datetime(2026, 3, 2, 10, 0),
                ),
                ConversionPath(
                    conversion_id="conv-2",
                    profile_id="p-2",
                    conversion_key=None,
                    conversion_ts=datetime(2026, 3, 3, 12, 0),
                    path_json={
                        "customer_id": "p-2",
                        "touchpoints": [
                            {"channel": "email", "timestamp": "2026-03-03T11:00:00Z"},
                        ],
                        "converted": False,
                        "conversion_value": 0.0,
                        "kpi_type": "add_to_cart",
                        "conversions": [],
                    },
                    path_hash="hash-2",
                    length=1,
                    first_touch_ts=datetime(2026, 3, 3, 11, 0),
                    last_touch_ts=datetime(2026, 3, 3, 11, 0),
                ),
                ConversionPath(
                    conversion_id="conv-3",
                    profile_id="p-3",
                    conversion_key="lead",
                    conversion_ts=datetime(2026, 3, 4, 12, 0),
                    path_json={
                        "customer_id": "p-3",
                        "touchpoints": [
                            {"channel": "whatsapp", "timestamp": "2026-03-04T08:30:00Z"},
                        ],
                        "converted": True,
                        "conversion_value": 42.0,
                        "kpi_type": "lead",
                        "conversions": [{"name": "lead", "value": 42.0}],
                    },
                    path_hash="hash-3",
                    length=1,
                    first_touch_ts=datetime(2026, 3, 4, 8, 30),
                    last_touch_ts=datetime(2026, 3, 4, 8, 30),
                ),
                ConversionPath(
                    conversion_id="conv-4",
                    profile_id="p-4",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 4, 2, 12, 0),
                    path_json={
                        "customer_id": "p-4",
                        "touchpoints": [
                            {"channel": "google_ads", "timestamp": "2026-04-02T09:00:00Z"},
                        ],
                        "converted": True,
                        "conversion_value": 90.0,
                        "kpi_type": "purchase",
                        "conversions": [{"name": "purchase", "value": 90.0}],
                    },
                    path_hash="hash-4",
                    length=1,
                    first_touch_ts=datetime(2026, 4, 2, 9, 0),
                    last_touch_ts=datetime(2026, 4, 2, 9, 0),
                ),
            ]
        )
        session.commit()

        with TestClient(app) as client:
            response = client.get("/api/experiments/setup-context?date_from=2026-03-01&date_to=2026-03-31")
        assert response.status_code == 200
        payload = response.json()

        assert payload["defaults"]["channel"] == "email"
        assert payload["defaults"]["conversion_key"] == "purchase"
        assert payload["summary"]["journeys"] == 3
        assert payload["summary"]["non_converted_journeys"] == 1

        channels = {row["channel"]: row for row in payload["channels"]}
        assert "email" in channels
        assert channels["email"]["eligible"] is True
        assert channels["email"]["journeys"] == 2
        assert channels["email"]["baseline_conversion_rate"] == 0.5
        assert channels["email"]["delivery_class"] == "owned"
        assert "google_ads" not in channels
        assert channels["direct"]["eligible"] is False

        kpis = {row["id"]: row for row in payload["kpis"]}
        assert kpis["purchase"]["count"] == 1
        assert kpis["lead"]["count"] == 1
        assert kpis["add_to_cart"]["count"] == 1
    finally:
        app.dependency_overrides.clear()
        main_module.KPI_CONFIG = original_kpi_config
        session.close()
        engine.dispose()


def test_incrementality_create_persists_structured_setup_and_config_snapshot():
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

    session = SessionLocal()
    try:
        invalidate_journey_cache()
        from app.models_config_dq import ModelConfig

        session.add(
            ModelConfig(
                id="cfg-1",
                name="Active config",
                status="active",
                version=4,
                config_json={"conversion_key": "purchase"},
                created_by="qa",
            )
        )
        session.commit()

        payload = {
            "name": "Email holdout",
            "channel": "email",
            "start_at": "2026-03-01T00:00:00Z",
            "end_at": "2026-03-21T00:00:00Z",
            "conversion_key": "purchase",
            "notes": "Operator note",
            "setup_source": "planner_setup_context",
            "assignment_unit": "profile_id",
            "assignment_method": "deterministic_hash",
            "treatment_rate": 0.9,
            "baseline_rate_estimate": 0.12,
            "min_runtime_days": 14,
            "exclusion_window_days": 30,
            "stop_rule": "Run 14 days and until sample target is met",
            "alpha": 0.05,
            "power": 0.8,
            "mde_target": 0.01,
            "config_id": "cfg-1",
        }

        with TestClient(app) as client:
            created = client.post("/api/experiments", json=payload)
            assert created.status_code == 200
            created_payload = created.json()
            assert created_payload["config_id"] == "cfg-1"
            assert created_payload["config_version"] == 4

            detail = client.get(f"/api/experiments/{created_payload['id']}")
            assert detail.status_code == 200
            detail_payload = detail.json()

        assert detail_payload["policy"]["setup_source"] == "planner_setup_context"
        assert detail_payload["policy"]["treatment_rate"] == 0.9
        assert detail_payload["guardrails"]["min_runtime_days"] == 14
        assert detail_payload["guardrails"]["exclusion_window_days"] == 30
        assert detail_payload["guardrails"]["power_plan"]["mde"] == 0.01
        assert detail_payload["setup"]["assignment_unit"] == "profile_id"
        assert detail_payload["setup"]["config_id"] == "cfg-1"
        assert detail_payload["setup"]["config_version"] == 4
    finally:
        app.dependency_overrides.clear()
        main_module.KPI_CONFIG = original_kpi_config
        session.close()
        engine.dispose()


def test_incrementality_recommend_design_returns_guided_plan():
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

    session = SessionLocal()
    try:
        invalidate_journey_cache()
        rows = []
        for idx in range(1, 61):
            converted = idx % 5 == 0
            rows.append(
                ConversionPath(
                    conversion_id=f"email-{idx}",
                    profile_id=f"profile-{idx}",
                    conversion_key="purchase" if converted else None,
                    conversion_ts=datetime(2026, 3, (idx % 10) + 1, 12, 0),
                    path_json={
                        "customer_id": f"profile-{idx}",
                        "touchpoints": [
                            {"channel": "email", "timestamp": f"2026-03-{(idx % 10) + 1:02d}T09:00:00Z"},
                        ],
                        "converted": converted,
                        "conversion_value": 100.0 if converted else 0.0,
                        "kpi_type": "purchase" if converted else "add_to_cart",
                        "conversions": [{"name": "purchase", "value": 100.0}] if converted else [],
                    },
                    path_hash=f"hash-{idx}",
                    length=1,
                    first_touch_ts=datetime(2026, 3, (idx % 10) + 1, 9, 0),
                    last_touch_ts=datetime(2026, 3, (idx % 10) + 1, 9, 0),
                )
            )
        session.add_all(rows)
        session.commit()

        with TestClient(app) as client:
            response = client.get(
                "/api/experiments/recommend-design?channel=email&conversion_key=purchase&date_from=2026-03-01&date_to=2026-03-10&mde=0.15"
            )
        assert response.status_code == 200
        payload = response.json()

        assert payload["channel"] == "email"
        assert payload["conversion_key"] == "purchase"
        assert payload["observed"]["journeys"] == 60
        assert payload["observed"]["matching_kpi_conversions"] == 12
        assert payload["observed"]["baseline_conversion_rate"] == 0.2
        assert payload["recommendation"]["treatment_rate"] in {0.9, 0.8, 0.7, 0.5}
        assert payload["recommendation"]["sample_target_total"] > 0
        assert payload["recommendation"]["min_runtime_days"] >= 14
        assert payload["recommendation"]["readiness"] in {"ready_to_launch", "needs_more_volume", "insufficient_signal"}
    finally:
        app.dependency_overrides.clear()
        main_module.KPI_CONFIG = original_kpi_config
        session.close()
        engine.dispose()


def test_incrementality_health_uses_planned_sample_and_runtime():
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

    session = SessionLocal()
    try:
        invalidate_journey_cache()
        with TestClient(app) as client:
            created = client.post(
                "/api/experiments",
                json={
                    "name": "Email health plan",
                    "channel": "email",
                    "start_at": "2026-03-01T00:00:00Z",
                    "end_at": "2026-04-30T00:00:00Z",
                    "conversion_key": "purchase",
                    "treatment_rate": 0.8,
                    "baseline_rate_estimate": 0.15,
                    "min_runtime_days": 30,
                    "alpha": 0.05,
                    "power": 0.8,
                    "mde_target": 0.15,
                },
            )
            assert created.status_code == 200
            exp_id = created.json()["id"]

            assigned = client.post(
                f"/api/experiments/{exp_id}/assign",
                json={"profile_ids": [f"p-{idx}" for idx in range(1, 11)], "treatment_rate": 0.8},
            )
            assert assigned.status_code == 200

            outcomes = client.post(
                f"/api/experiments/{exp_id}/outcomes",
                json={
                    "outcomes": [
                        {"profile_id": "p-1", "conversion_ts": "2026-03-10T12:00:00Z", "value": 100.0},
                        {"profile_id": "p-2", "conversion_ts": "2026-03-10T12:05:00Z", "value": 100.0},
                    ]
                },
            )
            assert outcomes.status_code == 200

            health = client.get(f"/api/experiments/{exp_id}/health")
            assert health.status_code == 200
            payload = health.json()

        assert payload["plan"]["treatment_rate"] == 0.8
        assert payload["plan"]["sample_target_total"] is not None
        assert payload["plan"]["sample_target_treatment"] is not None
        assert payload["plan"]["sample_target_control"] is not None
        assert payload["plan"]["sample_target_status"] == "warn"
        assert payload["runtime"]["planned_min_days"] == 30
        assert payload["ready_state"]["label"] in {"not_ready", "early"}
        assert any("Planned treatment sample target" in reason for reason in payload["ready_state"]["reasons"])
    finally:
        app.dependency_overrides.clear()
        main_module.KPI_CONFIG = original_kpi_config
        session.close()
        engine.dispose()
