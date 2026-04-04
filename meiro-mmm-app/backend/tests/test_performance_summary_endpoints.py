from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
from app.modules.performance import router as performance_router


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


def test_campaign_suggestions_payload_keeps_promoted_policy():
    journeys = [
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Paid Search", "campaign": "Brand"},
                {"timestamp": "2026-02-10T10:05:00Z", "channel": "Email", "campaign": "Checkout Rescue"},
            ],
            "conversions": [{"id": "conv-1", "value": 100.0}],
        }
    ]
    payload = performance_router._build_campaign_suggestions_payload(
        journeys=journeys,
        settings=performance_router.NBASettings(
            min_prefix_support=1,
            min_conversion_rate=0.5,
            max_prefix_depth=5,
            min_next_support=5,
            max_suggestions_per_prefix=3,
            min_uplift_pct=0.1,
            excluded_channels=["direct"],
            promoted_journey_policies=[
                {
                    "hypothesis_id": "hyp-campaign-policy",
                    "title": "Promote checkout rescue",
                    "journey_definition_id": "jd-1",
                    "prefix": "Paid Search:Brand",
                    "prefix_steps": ["Paid Search:Brand"],
                    "step": "Email:Checkout Rescue",
                    "channel": "Email",
                    "campaign": "Checkout Rescue",
                }
            ],
        ),
    )

    rec = payload["items"]["Paid Search:Brand"]
    assert rec["step"] == "Email:Checkout Rescue"
    assert rec["is_promoted_policy"] is True
    assert rec["promoted_policy_hypothesis_id"] == "hyp-campaign-policy"
    assert rec["promoted_policy_journey_definition_id"] == "jd-1"


def test_channel_suggestions_payload_keeps_promoted_policy():
    journeys = [
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Paid Search", "campaign": "Brand"},
                {"timestamp": "2026-02-10T10:05:00Z", "channel": "Email", "campaign": "Checkout Rescue"},
            ],
            "conversions": [{"id": "conv-1", "value": 100.0}],
        }
    ]
    payload = performance_router._build_channel_suggestions_payload(
        journeys=journeys,
        settings=performance_router.NBASettings(
            min_prefix_support=1,
            min_conversion_rate=0.5,
            max_prefix_depth=5,
            min_next_support=5,
            max_suggestions_per_prefix=3,
            min_uplift_pct=0.1,
            excluded_channels=["direct"],
            promoted_journey_policies=[
                {
                    "hypothesis_id": "hyp-channel-policy",
                    "title": "Promote checkout rescue email",
                    "journey_definition_id": "jd-1",
                    "prefix": "Paid Search",
                    "prefix_steps": ["Paid Search"],
                    "step": "Email",
                    "channel": "Email",
                    "campaign": None,
                }
            ],
        ),
    )

    assert "" not in payload["items"]
    rec = payload["items"]["Paid Search"]
    assert rec["step"] == "Email"
    assert rec["is_promoted_policy"] is True
    assert rec["promoted_policy_hypothesis_id"] == "hyp-channel-policy"
    assert rec["promoted_policy_journey_definition_id"] == "jd-1"


def test_filter_journeys_for_campaign_suggestions_respects_period_channels_and_conversion_key():
    journeys = [
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Paid Search", "campaign": "Brand"},
            ],
        },
        {
            "converted": True,
            "kpi_type": "lead",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Paid Search", "campaign": "Lead Gen"},
            ],
        },
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-01-10T10:00:00Z", "channel": "Paid Search", "campaign": "Old"},
            ],
        },
        {
            "converted": True,
            "kpi_type": "purchase",
            "touchpoints": [
                {"timestamp": "2026-02-10T10:00:00Z", "channel": "Organic", "campaign": "SEO"},
            ],
        },
    ]

    filtered = performance_router._filter_journeys_for_campaign_suggestions(
        journeys=journeys,
        date_from="2026-02-01",
        date_to="2026-02-14",
        timezone="UTC",
        channels=["Paid Search"],
        conversion_key="purchase",
    )

    assert len(filtered) == 1
    assert filtered[0]["touchpoints"][-1]["campaign"] == "Brand"


def test_resolve_effective_conversion_key_falls_back_when_configured_key_has_no_data(monkeypatch):
    def fake_iter_canonical_conversion_rows(db, *, date_from=None, date_to=None, conversion_key=None):
        if conversion_key == "form_submit":
            return iter(())
        return iter([SimpleNamespace(conversion_id="conv-1")])

    monkeypatch.setattr(
        performance_router,
        "iter_canonical_conversion_rows",
        fake_iter_canonical_conversion_rows,
    )

    effective_key, resolution = performance_router._resolve_effective_conversion_key(
        db=None,
        requested_conversion_key=None,
        configured_conversion_key="form_submit",
        date_from="2026-04-01",
        date_to="2026-04-01",
    )

    assert effective_key is None
    assert resolution == {
        "requested_conversion_key": None,
        "configured_conversion_key": "form_submit",
        "applied_conversion_key": None,
        "reason": "configured_conversion_key_has_no_data_in_selected_period",
    }
