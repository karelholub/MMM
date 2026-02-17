from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models_config_dq import JourneyDefinition, JourneyPathDaily


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


def _seed(session_factory):
    db = session_factory()
    try:
        jd = JourneyDefinition(
            id="jd-1",
            name="Journey A",
            description="A",
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

        rows = [
            JourneyPathDaily(
                date=date(2026, 1, 1),
                journey_definition_id="jd-1",
                path_hash="a",
                path_steps=["paid_search", "email"],
                path_length=2,
                count_journeys=20,
                count_conversions=10,
                avg_time_to_convert_sec=100.0,
                p50_time_to_convert_sec=90.0,
                p90_time_to_convert_sec=180.0,
                channel_group="paid",
                campaign_id="cmp-1",
                device="mobile",
                country="US",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
            JourneyPathDaily(
                date=date(2026, 1, 2),
                journey_definition_id="jd-1",
                path_hash="b",
                path_steps=["organic", "direct"],
                path_length=2,
                count_journeys=15,
                count_conversions=0,
                avg_time_to_convert_sec=0.0,
                p50_time_to_convert_sec=0.0,
                p90_time_to_convert_sec=0.0,
                channel_group="organic",
                campaign_id="cmp-2",
                device="desktop",
                country="US",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
            JourneyPathDaily(
                date=date(2026, 1, 3),
                journey_definition_id="jd-1",
                path_hash="c",
                path_steps=["paid_social", "email"],
                path_length=2,
                count_journeys=8,
                count_conversions=4,
                avg_time_to_convert_sec=220.0,
                p50_time_to_convert_sec=200.0,
                p90_time_to_convert_sec=350.0,
                channel_group="paid",
                campaign_id="cmp-1",
                device="mobile",
                country="CA",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
        ]
        db.add_all(rows)
        db.commit()
    finally:
        db.close()


def test_journey_paths_filters_and_mode(client):
    test_client, session_factory = client
    _seed(session_factory)

    base_params = {"date_from": "2026-01-01", "date_to": "2026-01-31", "limit": 50}
    resp = test_client.get("/api/journeys/jd-1/paths", params=base_params)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["mode"] == "conversion_only"
    assert payload["total"] == 2  # excludes row with 0 conversions
    assert all(item["count_conversions"] > 0 for item in payload["items"])
    assert "conversion_rate" in payload["items"][0]

    all_mode = test_client.get(
        "/api/journeys/jd-1/paths",
        params={**base_params, "mode": "all_journeys"},
    )
    assert all_mode.status_code == 200
    assert all_mode.json()["total"] == 3

    filtered = test_client.get(
        "/api/journeys/jd-1/paths",
        params={
            **base_params,
            "mode": "all_journeys",
            "channel_group": "paid",
            "campaign_id": "cmp-1",
            "device": "mobile",
            "country": "US",
        },
    )
    assert filtered.status_code == 200
    out = filtered.json()
    assert out["total"] == 1
    assert out["items"][0]["path_hash"] == "a"


def test_journey_paths_pagination_and_limits(client):
    test_client, session_factory = client
    _seed(session_factory)

    resp_page1 = test_client.get(
        "/api/journeys/jd-1/paths",
        params={
            "date_from": "2026-01-01",
            "date_to": "2026-01-31",
            "mode": "all_journeys",
            "limit": 1,
            "page": 1,
        },
    )
    assert resp_page1.status_code == 200
    body1 = resp_page1.json()
    assert body1["total"] == 3
    assert body1["limit"] == 1
    assert body1["page"] == 1
    assert len(body1["items"]) == 1

    resp_page2 = test_client.get(
        "/api/journeys/jd-1/paths",
        params={
            "date_from": "2026-01-01",
            "date_to": "2026-01-31",
            "mode": "all_journeys",
            "limit": 1,
            "page": 2,
        },
    )
    assert resp_page2.status_code == 200
    body2 = resp_page2.json()
    assert len(body2["items"]) == 1
    assert body2["items"][0]["path_hash"] != body1["items"][0]["path_hash"]

    invalid_limit = test_client.get(
        "/api/journeys/jd-1/paths",
        params={
            "date_from": "2026-01-01",
            "date_to": "2026-01-31",
            "limit": 1000,
        },
    )
    assert invalid_limit.status_code == 422
