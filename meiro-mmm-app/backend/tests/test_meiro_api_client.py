from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import services_meiro_api as meiro_api
from app.main import app


class FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


@pytest.fixture(autouse=True)
def meiro_api_env(monkeypatch: pytest.MonkeyPatch):
    meiro_api.clear_token_cache()
    monkeypatch.setenv("MEIRO_DOMAIN", "https://example.meiro")
    monkeypatch.setenv("MEIRO_TARGET_INSTANCE_URL", "https://example.meiro")
    monkeypatch.setenv("MEIRO_USERNAME", "api-user@example.com")
    monkeypatch.setenv("MEIRO_PASSWORD", "secret-password")
    monkeypatch.setenv("MEIRO_TIMEOUT_MS", "15000")
    yield
    meiro_api.clear_token_cache()


def test_login_token_is_cached_and_sent_with_both_headers(monkeypatch: pytest.MonkeyPatch):
    login_calls = []
    get_calls = []

    def fake_post(url, *, json, timeout, headers):
        login_calls.append({"url": url, "json": json, "timeout": timeout, "headers": headers})
        return FakeResponse(200, {"token": "cached-token"})

    def fake_request(method, url, *, headers, params, timeout):
        get_calls.append({"method": method, "url": url, "headers": headers, "params": params, "timeout": timeout})
        return FakeResponse(200, {"data": []})

    monkeypatch.setattr(meiro_api.requests, "post", fake_post)
    monkeypatch.setattr(meiro_api.requests, "request", fake_request)

    meiro_api.list_native_campaigns(channel="email")
    meiro_api.list_native_campaigns(channel="email")

    assert len(login_calls) == 1
    assert login_calls[0]["url"] == "https://example.meiro/api/users/login"
    assert get_calls[0]["headers"]["X-Access-Token"] == "cached-token"
    assert get_calls[0]["headers"]["Authorization"] == "Bearer cached-token"


def test_campaign_list_normalizes_all_channels_and_compacts_large_raw_fields(monkeypatch: pytest.MonkeyPatch):
    def fake_post(url, *, json, timeout, headers):
        return FakeResponse(200, {"token": "campaign-token"})

    def fake_request(method, url, *, headers, params, timeout):
        path = url.removeprefix("https://example.meiro")
        payloads = {
            "/api/emails": {
                "data": [
                    {
                        "id": "email-1",
                        "name": "Welcome",
                        "modified": "2026-04-21T09:00:00Z",
                        "html": "<html>large</html>",
                        "frequency_cap": {"limit": 2},
                        "schedules": [{"segment_ids": ["seg-a"], "start_at": "2026-04-21T09:00:00Z"}],
                    }
                ]
            },
            "/api/emails/trash": {"data": [{"id": "email-old", "subject": "Old email"}]},
            "/api/push_notifications": {"data": [{"uuid": "push-1", "title": "Push"}]},
            "/api/push_notifications/trash": {"data": []},
            "/api/whatsapp_campaigns": {"data": [{"campaign_id": "wa-1", "label": "WhatsApp"}]},
            "/api/whatsapp_campaigns/trash": {"data": []},
        }
        return FakeResponse(200, payloads[path])

    monkeypatch.setattr(meiro_api.requests, "post", fake_post)
    monkeypatch.setattr(meiro_api.requests, "request", fake_request)

    result = meiro_api.list_native_campaigns(include_deleted=True)

    assert result["total"] == 4
    by_id = {item["id"]: item for item in result["items"]}
    assert by_id["email-1"]["channel"] == "email"
    assert by_id["push-1"]["channel"] == "push"
    assert by_id["wa-1"]["channel"] == "whatsapp"
    assert by_id["email-old"]["deleted"] is True
    assert by_id["email-1"]["modifiedAt"] == "2026-04-21T09:00:00Z"
    assert by_id["email-1"]["segmentIds"] == ["seg-a"]
    assert by_id["email-1"]["raw"]["html"] == "[omitted: large content field]"


def test_campaign_detail_keeps_full_raw_payload(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(meiro_api.requests, "post", lambda *args, **kwargs: FakeResponse(200, {"token": "detail-token"}))
    monkeypatch.setattr(
        meiro_api.requests,
        "request",
        lambda *args, **kwargs: FakeResponse(200, {"id": "email-1", "name": "Welcome", "html": "<html>full</html>"}),
    )

    result = meiro_api.get_native_campaign("email", "email-1")

    assert result["id"] == "email-1"
    assert result["raw"]["html"] == "<html>full</html>"


def test_wbs_profile_and_segment_lookup_are_normalized(monkeypatch: pytest.MonkeyPatch):
    requests_seen = []

    def fake_post(url, *, json, timeout, headers):
        return FakeResponse(200, {"token": "wbs-token"})

    def fake_request(method, url, *, headers, params, timeout):
        requests_seen.append({"url": url, "params": params})
        if url.endswith("/wbs/segments"):
            return FakeResponse(200, {"status": "ok", "segments": [{"id": "seg-a"}, {"segment_id": "seg-b"}]})
        return FakeResponse(
            200,
            {
                "status": "found",
                "data": {"customer_entity_id": "cust-1", "attributes": {"email": "user@example.com"}},
            },
        )

    monkeypatch.setattr(meiro_api.requests, "post", fake_post)
    monkeypatch.setattr(meiro_api.requests, "request", fake_request)

    profile = meiro_api.lookup_wbs_profile(attribute="stitching_meiro_id", value="abc", category_id="cat-1")
    segments = meiro_api.lookup_wbs_segments(attribute="stitching_meiro_id", value="abc")

    assert profile["status"] == "found"
    assert profile["customerEntityId"] == "cust-1"
    assert profile["returnedAttributes"] == {"email": "user@example.com"}
    assert segments["segmentIds"] == ["seg-a", "seg-b"]
    assert requests_seen[0]["params"]["category_id"] == "cat-1"


def test_invalid_wbs_attribute_returns_clear_validation_error(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(meiro_api.requests, "post", lambda *args, **kwargs: FakeResponse(200, {"token": "wbs-token"}))
    monkeypatch.setattr(meiro_api.requests, "request", lambda *args, **kwargs: FakeResponse(400, {"error": "invalid attribute"}))

    with pytest.raises(meiro_api.MeiroApiError) as excinfo:
        meiro_api.lookup_wbs_profile(attribute="email", value="user@example.com")

    assert excinfo.value.status_code == 400
    assert excinfo.value.code == "validation_error"
    assert "attribute is allowed" in excinfo.value.message


def test_internal_native_campaign_route_passes_query_parameters(monkeypatch: pytest.MonkeyPatch):
    captured = {}

    def fake_list_native_campaigns(**kwargs):
        captured.update(kwargs)
        return {"items": [], "total": 0, "limit": kwargs["limit"], "offset": kwargs["offset"]}

    monkeypatch.setattr(meiro_api, "list_native_campaigns", fake_list_native_campaigns)
    client = TestClient(app)

    response = client.get("/v1/meiro/native-campaigns?channel=email&limit=12&offset=3&q=welcome&includeDeleted=true")

    assert response.status_code == 200
    assert captured == {
        "channel": "email",
        "limit": 12,
        "offset": 3,
        "q": "welcome",
        "include_deleted": True,
    }


def test_internal_wbs_profile_route_maps_category_id(monkeypatch: pytest.MonkeyPatch):
    captured = {}

    def fake_lookup_wbs_profile(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "customerEntityId": "cust-1", "returnedAttributes": {}, "data": {}, "raw": {}}

    monkeypatch.setattr(meiro_api, "lookup_wbs_profile", fake_lookup_wbs_profile)
    client = TestClient(app)

    response = client.get("/v1/meiro/audience/profile?attribute=stitching_meiro_id&value=abc&categoryId=cat-1")

    assert response.status_code == 200
    assert captured == {"attribute": "stitching_meiro_id", "value": "abc", "category_id": "cat-1"}


def test_reporting_search_uses_meiro_brokered_metabase_session(monkeypatch: pytest.MonkeyPatch):
    login_calls = []
    session_calls = []

    class FakeSession:
        def post(self, url, *, headers, timeout):
            session_calls.append({"kind": "post", "url": url, "headers": headers, "timeout": timeout})
            return FakeResponse(200, {})

        def request(self, method, url, *, params, json, timeout):
            session_calls.append(
                {"kind": "request", "method": method, "url": url, "params": params, "json": json, "timeout": timeout}
            )
            return FakeResponse(
                200,
                {
                    "data": [
                        {
                            "id": 91,
                            "model": "dashboard",
                            "name": "Meiro Journey Canvas",
                            "collection": {"name": "NEW Meiro Journey Canvas"},
                            "updated_at": "2026-04-24T09:00:00Z",
                        }
                    ]
                },
            )

    def fake_post(url, *, json, timeout, headers):
        login_calls.append({"url": url, "json": json, "timeout": timeout, "headers": headers})
        return FakeResponse(200, {"token": "reports-token"})

    monkeypatch.setattr(meiro_api.requests, "post", fake_post)
    monkeypatch.setattr(meiro_api.requests, "Session", lambda: FakeSession())

    result = meiro_api.search_reporting_assets(q="journey", model="dashboard", limit=5)

    assert result["total"] == 1
    assert result["items"][0]["name"] == "Meiro Journey Canvas"
    assert login_calls[0]["url"] == "https://example.meiro/api/users/login"
    assert session_calls[0]["url"] == "https://example.meiro/api/users/metabase_login"
    assert session_calls[0]["headers"]["Authorization"] == "Bearer reports-token"
    assert session_calls[1]["url"] == "https://example.meiro/reports/api/search"
    assert session_calls[1]["params"] == {"q": "journey"}


def test_reporting_dashboard_card_and_query_are_normalized(monkeypatch: pytest.MonkeyPatch):
    class FakeSession:
        def post(self, url, *, headers, timeout):
            return FakeResponse(200, {})

        def request(self, method, url, *, params, json, timeout):
            path = url.removeprefix("https://example.meiro/reports/api")
            payloads = {
                "/dashboard/91": {
                    "id": 91,
                    "name": "Meiro Journey Canvas",
                    "collection": {"name": "NEW Meiro Journey Canvas"},
                    "parameters": [{"name": "Start Date", "slug": "start_date", "type": "date/single"}],
                    "dashcards": [{"card": {"id": 1786, "name": "Daily Sent & Revenue"}}],
                },
                "/card/1786": {
                    "id": 1786,
                    "name": "Daily Sent & Revenue",
                    "collection": {"name": "NEW Meiro Journey Canvas"},
                    "display": "combo",
                    "dataset_query": {
                        "database": 7,
                        "type": "native",
                        "native": {"template-tags": {"start_date": {}, "end_date": {}}},
                    },
                    "result_metadata": [{"name": "Date"}, {"name": "Revenue"}],
                },
                "/card/1786/query/json": [
                    {"Date": "2026-04-23", "Revenue": 0},
                    {"Date": "2026-04-24", "Revenue": 250},
                ],
            }
            return FakeResponse(200, payloads[path])

    monkeypatch.setattr(meiro_api.requests, "post", lambda *args, **kwargs: FakeResponse(200, {"token": "reports-token"}))
    monkeypatch.setattr(meiro_api.requests, "Session", lambda: FakeSession())

    dashboard = meiro_api.get_reporting_dashboard(91)
    card = meiro_api.get_reporting_card(1786)
    query = meiro_api.query_reporting_card_json(1786, parameters=[{"type": "date/single", "value": "2026-04-24"}], limit=1)

    assert dashboard["cards"] == [{"cardId": 1786, "name": "Daily Sent & Revenue"}]
    assert card["templateTags"] == ["start_date", "end_date"]
    assert card["resultColumns"] == ["Date", "Revenue"]
    assert query["cardId"] == 1786
    assert query["rowCount"] == 2
    assert query["rows"] == [{"Date": "2026-04-23", "Revenue": 0}]


def test_reporting_routes_raise_clear_error_when_meiro_redirects_to_not_authorized(monkeypatch: pytest.MonkeyPatch):
    class RedirectedResponse(FakeResponse):
        def __init__(self):
            super().__init__(200, "<!DOCTYPE html><html></html>")
            self.url = "https://example.meiro/not-authorized"
            self.history = [type("History", (), {"status_code": 302, "url": "https://example.meiro/reports/api/search"})()]
            self.headers = {"content-type": "text/html"}

        def json(self):
            raise ValueError("not json")

    class FakeSession:
        def post(self, url, *, headers, timeout):
            return FakeResponse(200, {})

        def request(self, method, url, *, params, json, timeout):
            return RedirectedResponse()

    monkeypatch.setattr(meiro_api.requests, "post", lambda *args, **kwargs: FakeResponse(200, {"token": "reports-token"}))
    monkeypatch.setattr(meiro_api.requests, "Session", lambda: FakeSession())

    with pytest.raises(meiro_api.MeiroApiError) as excinfo:
        meiro_api.search_reporting_assets(q="journey")

    assert excinfo.value.status_code == 403
    assert excinfo.value.code == "authorization_error"
    assert "not enabled" in excinfo.value.message


def test_internal_reporting_routes_map_parameters(monkeypatch: pytest.MonkeyPatch):
    captured = {}

    def fake_search_reporting_assets(**kwargs):
        captured["search"] = kwargs
        return {"items": [], "total": 0, "limit": kwargs["limit"], "query": kwargs["q"], "model": kwargs["model"]}

    def fake_query_reporting_card_json(card_id, *, parameters, limit):
        captured["query"] = {"card_id": card_id, "parameters": parameters, "limit": limit}
        return {"cardId": card_id, "rows": [], "rowCount": 0, "limit": limit}

    monkeypatch.setattr(meiro_api, "search_reporting_assets", fake_search_reporting_assets)
    monkeypatch.setattr(meiro_api, "query_reporting_card_json", fake_query_reporting_card_json)
    client = TestClient(app)

    response_search = client.get("/v1/meiro/reports/search?q=journey&model=dashboard&limit=7")
    response_query = client.post(
        "/v1/meiro/reports/card/1786/query-json?limit=3",
        json=[{"type": "date/single", "target": ["variable", ["template-tag", "start_date"]], "value": "2026-04-24"}],
    )

    assert response_search.status_code == 200
    assert response_query.status_code == 200
    assert captured["search"] == {"q": "journey", "model": "dashboard", "limit": 7}
    assert captured["query"] == {
        "card_id": 1786,
        "parameters": [{"type": "date/single", "target": ["variable", ["template-tag", "start_date"]], "value": "2026-04-24"}],
        "limit": 3,
    }
