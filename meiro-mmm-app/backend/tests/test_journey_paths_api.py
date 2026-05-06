from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models_config_dq import JourneyDefinition, JourneyDefinitionInstanceFact, JourneyPathDaily
from app import services_journey_dimensions, services_journey_path_outputs

ADMIN_HEADERS = {
    "X-User-Id": "test-admin",
    "X-User-Role": "admin",
}


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
                gross_conversions_total=10.0,
                net_conversions_total=9.0,
                gross_revenue_total=500.0,
                net_revenue_total=450.0,
                view_through_conversions_total=0.0,
                click_through_conversions_total=10.0,
                mixed_path_conversions_total=0.0,
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
                gross_conversions_total=0.0,
                net_conversions_total=0.0,
                gross_revenue_total=0.0,
                net_revenue_total=0.0,
                view_through_conversions_total=0.0,
                click_through_conversions_total=0.0,
                mixed_path_conversions_total=0.0,
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
                gross_conversions_total=4.0,
                net_conversions_total=4.0,
                gross_revenue_total=240.0,
                net_revenue_total=240.0,
                view_through_conversions_total=0.0,
                click_through_conversions_total=4.0,
                mixed_path_conversions_total=0.0,
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
    resp = test_client.get("/api/journeys/jd-1/paths", params=base_params, headers=ADMIN_HEADERS)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["mode"] == "conversion_only"
    assert payload["total"] == 2  # excludes row with 0 conversions
    assert all(item["count_conversions"] > 0 for item in payload["items"])
    assert "conversion_rate" in payload["items"][0]
    assert payload["items"][0]["gross_revenue"] == 500.0
    assert payload["items"][0]["net_revenue"] == 450.0
    assert payload["items"][0]["gross_revenue_per_conversion"] == 50.0
    assert payload["summary"] == {
        "count_journeys": 28,
        "count_conversions": 14,
        "conversion_rate": 0.5,
        "gross_revenue": 740.0,
        "net_revenue": 690.0,
        "gross_revenue_per_conversion": 52.86,
        "net_revenue_per_conversion": 49.29,
    }

    all_mode = test_client.get(
        "/api/journeys/jd-1/paths",
        params={**base_params, "mode": "all_journeys"},
        headers=ADMIN_HEADERS,
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
        headers=ADMIN_HEADERS,
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
        headers=ADMIN_HEADERS,
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
        headers=ADMIN_HEADERS,
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
        headers=ADMIN_HEADERS,
    )
    assert invalid_limit.status_code == 422


def test_journey_paths_exclude_out_of_scope_campaigns_from_daily_outputs(client, monkeypatch):
    test_client, session_factory = client
    _seed(session_factory)
    monkeypatch.setattr(services_journey_path_outputs, "site_scope_is_strict", lambda: True)
    monkeypatch.setattr(services_journey_path_outputs, "get_out_of_scope_campaign_labels", lambda: {"cmp-1"})

    resp = test_client.get(
        "/api/journeys/jd-1/paths",
        params={
            "date_from": "2026-01-01",
            "date_to": "2026-01-31",
            "mode": "all_journeys",
            "limit": 50,
        },
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["campaign_id"] == "cmp-2"
    assert body["summary"]["count_journeys"] == 15


def test_journey_dimensions_exclude_out_of_scope_campaigns_from_selectors(client, monkeypatch):
    test_client, session_factory = client
    db = session_factory()
    try:
        db.add(
            JourneyDefinition(
                id="jd-dim-scope",
                name="Journey Dimensions Scope",
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
        db.add_all(
            [
                JourneyDefinitionInstanceFact(
                    date=date(2026, 1, 10),
                    journey_definition_id="jd-dim-scope",
                    conversion_id="conv-target",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 1, 10, 12, 0, 0),
                    path_hash="target",
                    steps_json=["target"],
                    path_length=1,
                    channel_group="paid_search",
                    last_touch_channel="google_ads",
                    campaign_id="meiro-brand",
                    device="desktop",
                    country="CZ",
                    interaction_path_type="click_through",
                    time_to_convert_sec=60.0,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=100.0,
                    net_revenue_total=100.0,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                ),
                JourneyDefinitionInstanceFact(
                    date=date(2026, 1, 11),
                    journey_definition_id="jd-dim-scope",
                    conversion_id="conv-legacy",
                    profile_id="p-2",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 1, 11, 12, 0, 0),
                    path_hash="legacy",
                    steps_json=["legacy"],
                    path_length=1,
                    channel_group="paid_social",
                    last_touch_channel="facebook_ads",
                    campaign_id="mytimi legacy",
                    device="mobile",
                    country="CZ",
                    interaction_path_type="click_through",
                    time_to_convert_sec=120.0,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=100.0,
                    net_revenue_total=100.0,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                ),
            ]
        )
        db.commit()
    finally:
        db.close()

    monkeypatch.setattr(services_journey_dimensions, "site_scope_is_strict", lambda: True)
    monkeypatch.setattr(services_journey_dimensions, "get_out_of_scope_campaign_labels", lambda: {"mytimi legacy"})

    resp = test_client.get(
        "/api/journeys/jd-dim-scope/dimensions",
        params={"date_from": "2026-01-01", "date_to": "2026-01-31"},
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["summary"]["journey_rows"] == 1
    assert body["summary"]["scope_filter"]["campaign_selectors_filtered"] is True
    assert body["summary"]["scope_filter"]["out_of_scope_campaign_labels"] == 1
    assert body["campaigns"] == [{"value": "meiro-brand", "count": 1}]


def test_journey_paths_fall_back_to_definition_instance_facts_when_daily_rows_absent(client):
    test_client, session_factory = client
    db = session_factory()
    try:
        db.add(
            JourneyDefinition(
                id="jd-facts",
                name="Journey Facts",
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
        db.add_all(
            [
                JourneyDefinitionInstanceFact(
                    date=date(2026, 1, 10),
                    journey_definition_id="jd-facts",
                    conversion_id="conv-1",
                    profile_id="p-1",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 1, 10, 12, 0, 0),
                    path_hash="path-facts",
                    steps_json=["Paid Landing", "Checkout", "Purchase / Lead Won (conversion)"],
                    path_length=3,
                    channel_group="paid",
                    last_touch_channel="google_ads",
                    campaign_id="cmp-facts",
                    device="mobile",
                    country="US",
                    interaction_path_type="click_through",
                    time_to_convert_sec=120.0,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=120.0,
                    net_revenue_total=100.0,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                ),
                JourneyDefinitionInstanceFact(
                    date=date(2026, 1, 11),
                    journey_definition_id="jd-facts",
                    conversion_id="conv-2",
                    profile_id="p-2",
                    conversion_key="purchase",
                    conversion_ts=datetime(2026, 1, 11, 12, 0, 0),
                    path_hash="path-facts",
                    steps_json=["Paid Landing", "Checkout", "Purchase / Lead Won (conversion)"],
                    path_length=3,
                    channel_group="paid",
                    last_touch_channel="google_ads",
                    campaign_id="cmp-facts",
                    device="mobile",
                    country="US",
                    interaction_path_type="click_through",
                    time_to_convert_sec=240.0,
                    gross_conversions_total=1.0,
                    net_conversions_total=1.0,
                    gross_revenue_total=180.0,
                    net_revenue_total=150.0,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                ),
            ]
        )
        db.commit()
    finally:
        db.close()

    resp = test_client.get(
        "/api/journeys/jd-facts/paths",
        params={"date_from": "2026-01-01", "date_to": "2026-01-31", "limit": 50},
        headers=ADMIN_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["summary"]["count_journeys"] == 2
    assert body["summary"]["count_conversions"] == 2
    assert body["summary"]["gross_revenue"] == 300.0
    assert body["items"][0]["path_hash"] == "path-facts"
    assert body["items"][0]["p50_time_to_convert_sec"] == 180.0
