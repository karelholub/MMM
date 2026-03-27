"""Tests for Overview (Cover) Dashboard API endpoints."""

from datetime import datetime

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import ConversionPath
from app.main import app
from app.services_overview import get_overview_drivers, get_overview_funnels

client = TestClient(app)


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _admin_headers():
    return {"X-User-Role": "admin", "X-User-Id": "qa-admin"}


def test_overview_summary_returns_consistent_shape():
    """GET /api/overview/summary returns kpi_tiles, highlights, freshness."""
    resp = client.get(
        "/api/overview/summary",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "kpi_tiles" in body
    assert "highlights" in body
    assert "freshness" in body
    assert "outcomes" in body
    assert isinstance(body["kpi_tiles"], list)
    assert isinstance(body["highlights"], list)
    assert "last_touchpoint_ts" in body["freshness"]
    assert "last_conversion_ts" in body["freshness"]
    assert "ingest_lag_minutes" in body["freshness"]
    assert "current_period" in body
    assert "previous_period" in body
    for tile in body["kpi_tiles"]:
        assert "kpi_key" in tile
        assert "value" in tile
        assert "delta_pct" in tile
        assert "delta_abs" in tile
        assert "current_period" in tile
        assert "previous_period" in tile
        assert "series" in tile
        assert "series_prev" in tile
        assert "confidence_score" in tile
        assert "confidence_level" in tile
        assert "confidence_reasons" in tile
        assert tile["kpi_key"] in ("spend", "visits", "conversions", "revenue", "net_conversions", "net_revenue")


def test_overview_summary_previous_period_is_equal_length():
    resp = client.get(
        "/api/overview/summary",
        params={"date_from": "2024-02-01", "date_to": "2024-02-14"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    cp = body["current_period"]
    pp = body["previous_period"]
    cp_from = datetime.fromisoformat(cp["date_from"])
    cp_to = datetime.fromisoformat(cp["date_to"])
    pp_from = datetime.fromisoformat(pp["date_from"])
    pp_to = datetime.fromisoformat(pp["date_to"])
    assert (cp_to - cp_from) == (pp_to - pp_from)
    assert pp_to < cp_from


def test_overview_summary_empty_series_do_not_fake_flat_lines():
    resp = client.get(
        "/api/overview/summary",
        params={"date_from": "2024-03-01", "date_to": "2024-03-07"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    for tile in body["kpi_tiles"]:
        if tile["value"] == 0:
            assert tile["series"] == []


def test_overview_summary_optional_params():
    """Summary accepts timezone, currency, workspace, account, model_id."""
    resp = client.get(
        "/api/overview/summary",
        params={
            "date_from": "2024-01-01",
            "date_to": "2024-01-31",
            "timezone": "Europe/London",
            "currency": "USD",
            "workspace": "ws1",
            "model_id": "cfg-1",
        },
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("timezone") == "Europe/London"
    assert body.get("currency") == "USD"
    assert body.get("model_id") == "cfg-1"


def test_overview_summary_missing_dates_validation():
    """Summary requires date_from and date_to."""
    resp = client.get("/api/overview/summary", params={"date_from": "2024-01-01"}, headers=_admin_headers())
    assert resp.status_code == 422


def test_overview_drivers_returns_consistent_shape():
    """GET /api/overview/drivers returns by_channel, by_campaign, biggest_movers."""
    resp = client.get(
        "/api/overview/drivers",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "by_channel" in body
    assert "by_campaign" in body
    assert "biggest_movers" in body
    assert isinstance(body["by_channel"], list)
    assert isinstance(body["by_campaign"], list)
    assert isinstance(body["biggest_movers"], list)
    for ch in body["by_channel"]:
        assert "channel" in ch
        assert "spend" in ch
        assert "visits" in ch
        assert "conversions" in ch
        assert "revenue" in ch
        assert "outcomes" in ch


def test_overview_drivers_top_n():
    """Drivers accepts top_campaigns_n and conversion_key."""
    resp = client.get(
        "/api/overview/drivers",
        params={
            "date_from": "2024-01-01",
            "date_to": "2024-01-31",
            "top_campaigns_n": 5,
        },
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["by_campaign"]) <= 5


def test_overview_alerts_returns_consistent_shape():
    """GET /api/overview/alerts returns alerts list with deep_link."""
    resp = client.get("/api/overview/alerts", headers=_admin_headers())
    assert resp.status_code == 200
    body = resp.json()
    assert "alerts" in body
    assert "total" in body
    assert isinstance(body["alerts"], list)
    for a in body["alerts"]:
        assert "id" in a
        assert "ts_detected" in a
        assert "deep_link" in a
        assert "status" in a


def test_overview_alerts_optional_filters():
    """Alerts accepts scope, status, limit."""
    resp = client.get(
        "/api/overview/alerts",
        params={"scope": "default", "limit": 10},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["alerts"]) <= 10


def test_overview_drivers_last_touch_count_uses_position_not_value_equality():
    db = _unit_db_session()
    try:
        row = ConversionPath(
            conversion_id="conv-1",
            profile_id="p-1",
            conversion_key="signup",
            conversion_ts=datetime(2024, 2, 15, 12, 0),
            path_json={
                "conversion_value": 100.0,
                "touchpoints": [
                    {"channel": "email", "campaign": "camp-a"},
                    {"channel": "email", "campaign": "camp-a"},
                ],
            },
            path_hash="hash-1",
            length=2,
            first_touch_ts=datetime(2024, 2, 15, 11, 0),
            last_touch_ts=datetime(2024, 2, 15, 11, 30),
        )
        db.add(row)
        db.commit()

        out = get_overview_drivers(
            db,
            date_from="2024-02-01",
            date_to="2024-02-29",
            expenses={},
            top_campaigns_n=10,
        )
        by_channel = {x["channel"]: x for x in out["by_channel"]}
        assert by_channel["email"]["conversions"] == 1
    finally:
        db.close()


def test_overview_funnels_ranks_paths_by_conversions_revenue_and_speed():
    db = _unit_db_session()
    try:
        rows = [
            ConversionPath(
                conversion_id="conv-1",
                profile_id="p-1",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 10, 12, 0),
                path_json={
                    "conversion_value": 100.0,
                    "touchpoints": [
                        {"channel": "paid_social"},
                        {"channel": "direct"},
                    ],
                },
                path_hash="hash-1",
                length=2,
                first_touch_ts=datetime(2024, 2, 8, 12, 0),
                last_touch_ts=datetime(2024, 2, 10, 11, 0),
            ),
            ConversionPath(
                conversion_id="conv-2",
                profile_id="p-2",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 11, 12, 0),
                path_json={
                    "conversion_value": 90.0,
                    "touchpoints": [
                        {"channel": "paid_social"},
                        {"channel": "direct"},
                    ],
                },
                path_hash="hash-2",
                length=2,
                first_touch_ts=datetime(2024, 2, 10, 12, 0),
                last_touch_ts=datetime(2024, 2, 11, 11, 0),
            ),
            ConversionPath(
                conversion_id="conv-3",
                profile_id="p-3",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 12, 12, 0),
                path_json={
                    "conversion_value": 400.0,
                    "touchpoints": [
                        {"channel": "email"},
                        {"channel": "direct"},
                    ],
                },
                path_hash="hash-3",
                length=2,
                first_touch_ts=datetime(2024, 2, 12, 6, 0),
                last_touch_ts=datetime(2024, 2, 12, 11, 0),
            ),
            ConversionPath(
                conversion_id="conv-4",
                profile_id="p-4",
                conversion_key="purchase",
                conversion_ts=datetime(2024, 2, 13, 12, 0),
                path_json={
                    "conversion_value": 40.0,
                    "touchpoints": [
                        {"channel": "organic_search"},
                        {"channel": "email"},
                    ],
                },
                path_hash="hash-4",
                length=2,
                first_touch_ts=datetime(2024, 2, 13, 10, 0),
                last_touch_ts=datetime(2024, 2, 13, 11, 0),
            ),
        ]
        for row in rows:
            db.add(row)
        db.commit()

        out = get_overview_funnels(
            db,
            date_from="2024-02-01",
            date_to="2024-02-29",
            limit=5,
        )

        assert out["summary"]["total_conversions"] == 4
        assert "net_conversions" in out["summary"]
        assert "gross_revenue" in out["summary"]
        assert out["tabs"]["conversions"][0]["path"] == "paid_social > direct"
        assert out["tabs"]["conversions"][0]["conversions"] == 2
        assert out["tabs"]["revenue"][0]["path"] == "email > direct"
        assert out["tabs"]["revenue"][0]["revenue"] == 400.0
        assert out["tabs"]["speed"][0]["path"] == "email > direct"
        assert out["tabs"]["speed"][0]["median_days_to_convert"] < out["tabs"]["conversions"][0]["median_days_to_convert"]
        assert out["summary"]["top_paths_conversion_share"] == 1.0
    finally:
        db.close()


def test_overview_funnels_endpoint_returns_expected_shape():
    resp = client.get(
        "/api/overview/funnels",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
        headers=_admin_headers(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "summary" in body
    assert "tabs" in body
    assert set(body["tabs"].keys()) == {"conversions", "revenue", "speed"}
