from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _admin_headers():
    return {"X-User-Role": "admin", "X-User-Id": "qa-admin"}


def test_channel_trend_requires_attribution_view_permission():
    res = client.get(
        "/api/performance/channel/trend",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14", "kpi_key": "revenue"},
    )
    assert res.status_code == 403
    body = res.json()
    assert body["detail"]["permission"] == "attribution.view"


def test_channel_trend_normalizes_channel_filters_and_query_context_meta():
    res = client.get(
        "/api/performance/channel/trend",
        params={
            "date_from": "2026-02-01",
            "date_to": "2026-02-14",
            "kpi_key": "revenue",
            "grain": "auto",
            "channels": ["google_ads", "meta_ads, google_ads", "  all  "],
        },
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert "meta" in body
    assert body["meta"]["channels"] == []
    assert body["meta"]["query_context"]["current_period"]["date_from"] == "2026-02-01"
    assert body["meta"]["query_context"]["previous_period"]["date_to"] == "2026-01-31"


def test_campaign_trend_normalizes_channels_and_adds_query_context_meta():
    res = client.get(
        "/api/performance/campaign/trend",
        params={
            "date_from": "2026-02-01",
            "date_to": "2026-02-14",
            "kpi_key": "conversions",
            "conversion_key": "purchase",
            "channels": ["meta_ads", "google_ads"],
            "compare": "1",
        },
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert sorted(body["meta"]["channels"]) == ["google_ads", "meta_ads"]
    assert body["meta"]["conversion_key"] == "purchase"
    assert body["meta"]["query_context"]["compare"] is True
    assert body["meta"]["query_context"]["current_period"]["grain"] in ("daily", "weekly")
