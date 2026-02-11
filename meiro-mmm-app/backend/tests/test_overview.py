"""Tests for Overview (Cover) Dashboard API endpoints."""

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_overview_summary_returns_consistent_shape():
    """GET /api/overview/summary returns kpi_tiles, highlights, freshness."""
    resp = client.get(
        "/api/overview/summary",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "kpi_tiles" in body
    assert "highlights" in body
    assert "freshness" in body
    assert isinstance(body["kpi_tiles"], list)
    assert isinstance(body["highlights"], list)
    assert "last_touchpoint_ts" in body["freshness"]
    assert "last_conversion_ts" in body["freshness"]
    assert "ingest_lag_minutes" in body["freshness"]
    for tile in body["kpi_tiles"]:
        assert "kpi_key" in tile
        assert "value" in tile
        assert "confidence" in tile
        assert tile["kpi_key"] in ("spend", "conversions", "revenue")


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
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("timezone") == "Europe/London"
    assert body.get("currency") == "USD"
    assert body.get("model_id") == "cfg-1"


def test_overview_summary_missing_dates_validation():
    """Summary requires date_from and date_to."""
    resp = client.get("/api/overview/summary", params={"date_from": "2024-01-01"})
    assert resp.status_code == 422


def test_overview_drivers_returns_consistent_shape():
    """GET /api/overview/drivers returns by_channel, by_campaign, biggest_movers."""
    resp = client.get(
        "/api/overview/drivers",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
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
        assert "conversions" in ch
        assert "revenue" in ch


def test_overview_drivers_top_n():
    """Drivers accepts top_campaigns_n and conversion_key."""
    resp = client.get(
        "/api/overview/drivers",
        params={
            "date_from": "2024-01-01",
            "date_to": "2024-01-31",
            "top_campaigns_n": 5,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["by_campaign"]) <= 5


def test_overview_alerts_returns_consistent_shape():
    """GET /api/overview/alerts returns alerts list with deep_link."""
    resp = client.get("/api/overview/alerts")
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
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["alerts"]) <= 10
