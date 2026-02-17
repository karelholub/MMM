from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _admin_headers():
    return {"X-User-Role": "admin", "X-User-Id": "qa-admin"}


def test_channel_summary_requires_attribution_view_permission():
    res = client.get(
        "/api/performance/channel/summary",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14"},
    )
    assert res.status_code == 403
    assert res.json()["detail"]["permission"] == "attribution.view"


def test_channel_summary_response_shape():
    res = client.get(
        "/api/performance/channel/summary",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14", "channels": ["google_ads,meta_ads"]},
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert "current_period" in body
    assert "previous_period" in body
    assert "items" in body
    assert "totals" in body
    assert "config" in body
    assert "mapping_coverage" in body
    assert "meta" in body
    assert isinstance(body["meta"]["channels"], list)


def test_campaign_summary_response_shape_and_note():
    res = client.get(
        "/api/performance/campaign/summary",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14", "compare": "1"},
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert "items" in body
    assert "totals" in body
    assert "config" in body
    assert "mapping_coverage" in body
    assert "notes" in body
    assert body["notes"]
    assert "meta" in body
    assert "query_context" in body["meta"]


def test_campaign_summary_includes_conversion_key_meta_when_passed():
    res = client.get(
        "/api/performance/campaign/summary",
        params={"date_from": "2026-02-01", "date_to": "2026-02-14", "conversion_key": "purchase"},
        headers=_admin_headers(),
    )
    assert res.status_code == 200
    body = res.json()
    assert body["meta"]["conversion_key"] == "purchase"
