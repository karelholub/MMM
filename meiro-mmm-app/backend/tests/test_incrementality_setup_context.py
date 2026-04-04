from datetime import datetime

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
import app.main as main_module
from app.models_config_dq import ConversionPath
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
